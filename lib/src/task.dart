import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dtorrent_parser/dtorrent_parser.dart';
import 'package:dtorrent_task/src/file/download_file_manager_events.dart';
import 'package:dtorrent_task/src/httpserver/server.dart';
import 'package:dtorrent_task/src/lsd/lsd_events.dart';
import 'package:dtorrent_task/src/peer/protocol/peer_events.dart';
import 'package:dtorrent_task/src/peer/swarm/peers_manager_events.dart';
import 'package:dtorrent_task/src/piece/piece_base.dart';
import 'package:dtorrent_task/src/piece/piece_manager_events.dart';
import 'package:dtorrent_task/src/peer/protocol/peer_events.dart'
    as peer_events;
import 'package:dtorrent_task/src/piece/sequential_piece_selector.dart';
import 'package:dtorrent_task/src/task_events.dart';
import 'package:dtorrent_tracker/dtorrent_tracker.dart';
import 'package:dtorrent_common/dtorrent_common.dart';
import 'package:bittorrent_dht/bittorrent_dht.dart';
import 'package:logging/logging.dart';
import 'package:utp_protocol/utp_protocol.dart';
import 'package:events_emitter2/events_emitter2.dart';
import 'file/download_file_manager.dart';
import 'file/state_file.dart';
import 'lsd/lsd.dart';
import 'peer/protocol/peer.dart';
import 'piece/base_piece_selector.dart';
import 'piece/piece_manager.dart';
import 'peer/swarm/peers_manager.dart';
import 'utils.dart';

const MAX_PEERS = 50;
const MAX_IN_PEERS = 10;

enum TaskState { running, paused, stopped }

var _log = Logger('TorrentTask');

abstract class TorrentTask with EventsEmittable<TaskEvent> {
  factory TorrentTask.newTask(Torrent metaInfo, String savePath,
      [bool stream = false]) {
    return _TorrentTask(
      metaInfo,
      savePath,
      stream: stream,
    );
  }
  void startAnnounceUrl(Uri url, Uint8List infoHash);
  Torrent get metaInfo;

  // The name of the torrent
  String get name => metaInfo.name;

  // The dht instance

  DHT? get dht;

  int get allPeersNumber;

  int get connectedPeersNumber;

  int get seederNumber;

  /// Current download speed
  double get currentDownloadSpeed;

  /// Current upload speed
  double get uploadSpeed;

  /// Average download speed
  double get averageDownloadSpeed;

  /// Average upload speed
  double get averageUploadSpeed;

  // TODO debug:
  double get utpDownloadSpeed;
  // TODO debug:
  double get utpUploadSpeed;
  // TODO debug:
  int get utpPeerCount;

  /// Downloaded total bytes length
  int? get downloaded;

  /// Downloaded percent
  double get progress;

  /// Start to download
  Future<Map> start();

  // Start streaming videos
  Future<void> startStreaming();

  /// Stop this task
  Future stop();

  // Dispose task object

  Future<void> dispose();

  abstract TaskState state;
  Iterable<Peer>? get activePeers;
  PieceManager? get pieceManager;

  /// Pause task
  void pause();

  /// Resume task
  void resume();

  void requestPeersFromDHT();

  /// Adding a DHT node usually involves adding the nodes from the torrent file into the DHT network.
  ///
  /// Alternatively, you can directly add known node addresses.
  void addDHTNode(Uri uri);

  /// Add known Peer addresses.
  void addPeer(CompactAddress address, PeerSource source,
      {PeerType? type, Socket socket});

  Stream<List<int>>? createStream({
    int filePosition = 0,
    int? endPosition,
    String? fileName,
  });
}

class _TorrentTask
    with EventsEmittable<TaskEvent>
    implements TorrentTask, AnnounceOptionsProvider {
  static InternetAddress LOCAL_ADDRESS =
      InternetAddress.fromRawAddress(Uint8List.fromList([127, 0, 0, 1]));

  TorrentAnnounceTracker? _tracker;

  DHT? _dht = DHT();

  @override
  // The Dht instance
  DHT? get dht => _dht;

  LSD? _lsd;

  StateFile? _stateFile;

  PieceManager? _pieceManager;

  @override
  PieceManager? get pieceManager => _pieceManager;

  DownloadFileManager? _fileManager;

  PeersManager? _peersManager;

  StreamingServer? _streamingServer;

  bool stream;

  /// The maximum size of the disk write cache.
  int maxWriteBufferSize;

  final _flushIndicesBuffer = <int>{};
  @override
  Iterable<Peer>? get activePeers => _peersManager?.activePeers;

  final Torrent _metaInfo;
  @override
  Torrent get metaInfo => _metaInfo;

  @override
  String get name => metaInfo.name;

  final String _savePath;

  final Set<String> _peerIds = {};

  late String
      _peerId; // This is the generated local peer ID, which is different from the ID used in the Peer class.

  ServerSocket? _serverSocket;

  StreamSubscription<Socket>? _serverSocketListener;
  ServerUTPSocket? _utpServer;

  final Set<InternetAddress> _comingIp = {};

  EventsListener<TorrentAnnounceEvent>? trackerListener;
  EventsListener<peer_events.PeerEvent>? peersManagerListener;
  EventsListener<DownloadFileManagerEvent>? fileManagerListener;
  EventsListener<PieceManagerEvent>? pieceManagerListener;
  EventsListener<LSDEvent>? lsdListener;
  EventsListener<DHTEvent>? _dhtListener;

  _TorrentTask(this._metaInfo, this._savePath,
      {this.maxWriteBufferSize = MAX_WRITE_BUFFER_SIZE, this.stream = false}) {
    _peerId = generatePeerId();
  }

  @override
  double get averageDownloadSpeed {
    if (_peersManager != null) {
      return _peersManager!.averageDownloadSpeed;
    } else {
      return 0.0;
    }
  }

  @override
  double get averageUploadSpeed {
    if (_peersManager != null) {
      return _peersManager!.averageUploadSpeed;
    } else {
      return 0.0;
    }
  }

  @override
  double get currentDownloadSpeed {
    if (_peersManager != null) {
      return _peersManager!.currentDownloadSpeed;
    } else {
      return 0.0;
    }
  }

  @override
  double get uploadSpeed {
    if (_peersManager != null) {
      return _peersManager!.uploadSpeed;
    } else {
      return 0.0;
    }
  }

  String? _infoHashString;

  Timer? _dhtRepeatTimer;

  Future<PeersManager> _init(Torrent model, String savePath) async {
    _lsd ??= LSD(model.infoHash, _peerId);
    _infoHashString ??= String.fromCharCodes(model.infoHashBuffer);
    _tracker ??= TorrentAnnounceTracker(this);
    _stateFile ??= await StateFile.getStateFile(savePath, model);
    _pieceManager ??= PieceManager.createPieceManager(
        stream ? SequentialPieceSelector() : BasePieceSelector(),
        model,
        _stateFile!.bitfield);
    _fileManager ??= await DownloadFileManager.createFileManager(
        model, savePath, _stateFile!, _pieceManager!.pieces.values.toList());
    _peersManager ??= PeersManager(_peerId, model);

    return _peersManager!;
  }

  void initStreaming() {
    _streamingServer ??= StreamingServer(_fileManager!, this);
  }

  @override
  Future<void> startStreaming() async {
    initStreaming();
    await _init(_metaInfo, _savePath);
    for (var file in _fileManager!.files) {
      await file.requestFlush();
    }
    if (!_streamingServer!.running) {
      await _streamingServer?.start().then((event) => events.emit(event));
    }
  }

  @override
  Stream<List<int>>? createStream(
      {int filePosition = 0, int? endPosition, String? fileName}) {
    if (_fileManager == null ||
        _peersManager == null ||
        _pieceManager == null) {
      return null;
    }
    TorrentFile? file;
    if (fileName != null) {
      file = _fileManager!.metainfo.files
          .firstWhere((file) => file.name == fileName);
    } else {
      file = _fileManager!.metainfo.files.firstWhere(
        (file) => file.name.contains('mp4'),
        orElse: () => _fileManager!.metainfo.files.first,
      );
    }
    var localFile = _fileManager?.files.firstWhere(
        (downloadedFile) => downloadedFile.originalFileName == file?.name);

    if (localFile == null) return null;
    // if no end position provided, read all file
    endPosition ??= file.length;

    var offsetStart = file.offset + filePosition;
    var offsetEnd = file.offset + endPosition;

    var startPieceIndex = offsetStart ~/ metaInfo.pieceLength;
    var endPieceIndex = offsetEnd ~/ metaInfo.pieceLength;

    _pieceManager!.pieceSelector.setPriorityPieces(
        {for (var i = startPieceIndex; i <= endPieceIndex; i++) i});

    var stream = localFile.createStream(filePosition, endPosition);
    if (stream == null) return null;

    return stream;
  }

  @override
  void addPeer(CompactAddress address, PeerSource source,
      {PeerType? type, Socket? socket}) {
    _peersManager?.addNewPeerAddress(address, source,
        type: type, socket: socket);
  }

  void _whenTaskDownloadComplete() async {
    await _peersManager
        ?.disposeAllSeeder('Download complete,disconnect seeder');
    await _tracker?.complete();
    events.emit(TaskCompleted());
  }

  void _whenFileDownloadComplete(DownloadManagerFileCompleted event) {
    events.emit(TaskFileCompleted(event.file));
  }

  void _processTrackerPeerEvent(AnnouncePeerEventEvent event) {
    if (event.event == null) return;
    var ps = event.event!.peers;
    if (ps.isNotEmpty) {
      for (var url in ps) {
        _processNewPeerFound(url, PeerSource.tracker);
      }
    }
  }

  void _processLSDPeerEvent(LSDNewPeer event) {
    print('There is LSD! !');
  }

  void _processNewPeerFound(CompactAddress url, PeerSource source) {
    _log.info(
      "Add new peer ${url.toString()} from ${source.name} to peersManager",
    );
    _peersManager?.addNewPeerAddress(url, source);
  }

  void _processDHTPeer(CompactAddress peer, String infoHash) {
    _log.fine(
      "Got new peer from $peer DHT for infohash: ${Uint8List.fromList(infoHash.codeUnits).toHexString()}",
    );
    if (infoHash == _infoHashString) {
      _processNewPeerFound(peer, PeerSource.dht);
    }
  }

  void _hookUTP(UTPSocket socket) {
    if (socket.remoteAddress == LOCAL_ADDRESS) {
      socket.close();
      return;
    }
    if (_comingIp.length >= MAX_IN_PEERS || !_comingIp.add(socket.remoteAddress)) {
      socket.close();
      return;
    }
    _log.info(
      'incoming connect: ${socket.remoteAddress.address}:${socket.remotePort}',
    );
    _peersManager?.addNewPeerAddress(
        CompactAddress(socket.remoteAddress, socket.remotePort),
        PeerSource.incoming,
        type: PeerType.UTP,
        socket: socket);
  }

  void _hookInPeer(Socket socket) {
    if (socket.remoteAddress == LOCAL_ADDRESS) {
      socket.close();
      return;
    }
    if (_comingIp.length >= MAX_IN_PEERS || !_comingIp.add(socket.remoteAddress)) {
      socket.close();
      return;
    }
    _log.info(
      'incoming connect: ${socket.remoteAddress.address}:${socket.remotePort}',
    );
    _peersManager?.addNewPeerAddress(
        CompactAddress(socket.remoteAddress, socket.remotePort),
        PeerSource.incoming,
        type: PeerType.TCP,
        socket: socket);
  }

  @override
  void pause() {
    if (state == TaskState.paused) return;
    state = TaskState.paused;
    _peersManager?.pause();
    events.emit(TaskPaused());
  }

  @override
  TaskState state = TaskState.stopped;

  @override
  void resume() {
    if (state == TaskState.paused) {
      state = TaskState.running;
      _peersManager?.resume();
      events.emit(TaskResumed());
    }
  }

  @override
  Future<Map> start() async {
    state = TaskState.running;
    // Incoming peer:
    _serverSocket ??= await ServerSocket.bind(InternetAddress.anyIPv4, 0);
    await _init(_metaInfo, _savePath);
    _serverSocketListener = _serverSocket?.listen(_hookInPeer);
    _utpServer ??= await ServerUTPSocket.bind(
        InternetAddress.anyIPv4, _serverSocket?.port ?? 0);
    _utpServer?.listen(_hookUTP);

    var map = {};
    map['name'] = _metaInfo.name;
    map['tcp_socket'] = _serverSocket?.port;
    map['complete_pieces'] = List.from(_stateFile!.bitfield.completedPieces);
    map['total_pieces_num'] = _stateFile!.bitfield.piecesNum;
    map['downloaded'] = _stateFile!.downloaded;
    map['uploaded'] = _stateFile!.uploaded;
    map['total_length'] = _metaInfo.length;
    // Outgoing peer:
    trackerListener = _tracker?.createListener();
    peersManagerListener = _peersManager?.createListener();
    fileManagerListener = _fileManager?.createListener();
    pieceManagerListener = _pieceManager?.createListener();
    lsdListener = _lsd?.createListener();
    trackerListener?.on<AnnouncePeerEventEvent>(_processTrackerPeerEvent);

    peersManagerListener
      ?..on<PeerAllowFast>(_processAllowFast)
      ..on<PeerRejectEvent>(_processRejectRequest)
      ..on<PeerDisposeEvent>(_processPeerDispose)
      ..on<PeerPieceEvent>(_processReceivePiece)
      ..on<PeerRequestEvent>(_processPeerRequest)
      ..on<PeerHandshakeEvent>(_processPeerHandshake)
      ..on<PeerBitfieldEvent>(_processBitfieldUpdate)
      ..on<PeerHaveAll>(_processHaveAll)
      ..on<PeerHaveNone>(_processHaveNone)
      ..on<PeerChokeChanged>(_processChokeChange)
      ..on<PeerHaveEvent>(_processHaveUpdate)
      ..on<RequestTimeoutEvent>(
          (event) => _processRequestTimeout(event.peer, event.requests))
      ..on<UpdateUploaded>(
          (event) => _fileManager?.updateUpload(event.uploaded));
    fileManagerListener
      ?..on<DownloadManagerFileCompleted>(_whenFileDownloadComplete)
      ..on<StateFileUpdated>((event) => events.emit(StateFileUpdated()))
      ..on<SubPieceReadCompleted>((event) => _peersManager
          ?.readSubPieceComplete(event.pieceIndex, event.begin, event.block));
    pieceManagerListener
      ?..on<PieceAccepted>((event) => processPieceAccepted(event.pieceIndex))
      ..on<PieceRejected>((event) => null);
    lsdListener?.on<LSDNewPeer>(_processLSDPeerEvent);
    _lsd?.port = _serverSocket?.port;
    _lsd?.start();
    _dhtListener = _dht?.createListener();
    _dhtListener?.on<NewPeerEvent>(
        (event) => _processDHTPeer(event.address, event.infoHash));
    _dht?.announce(
        String.fromCharCodes(_metaInfo.infoHashBuffer), _serverSocket!.port);

    _dht?.bootstrap();

    if (_fileManager != null && _fileManager!.isAllComplete) {
      _tracker?.complete();
    } else {
      _tracker?.runTrackers(_metaInfo.announces, _metaInfo.infoHashBuffer,
          event: EVENT_STARTED);
    }
    events.emit(TaskStarted());
    return map;
  }

  void processPieceRejected(int index) {
    var piece = _pieceManager?[index];
    if (piece == null) return;

    // TODO: still need optimizing for last pieces
    for (var peer in piece.availablePeers) {
      requestPieces(peer, piece.index);
    }
  }

  Future<void> processPieceAccepted(int index) async {
    var piece = _pieceManager?[index];
    if (piece == null || _fileManager == null || _pieceManager == null) return;

    var block = piece.flush();
    if (block == null) return;

    if (_fileManager!.localHave(index)) return;
    var written = await _fileManager!.writeFile(
      index,
      0,
      block,
    );

    if (!written) return;
    _pieceManager!.processPieceWriteComplete(index);
    await _fileManager!.updateBitfield(index);
    _peersManager?.sendHaveToAll(index);
    _flushIndicesBuffer.add(index);
    await _flushFiles(_flushIndicesBuffer);
    if (_fileManager!.isAllComplete) {
      events.emit(AllComplete());
      _whenTaskDownloadComplete();
    }
  }

  Future _flushFiles(Set<int> indices) async {
    if (indices.isEmpty || _fileManager == null) return;
    var piecesSize = _metaInfo.pieceLength;
    var buffer = indices.length * piecesSize;
    if (buffer >= maxWriteBufferSize || _fileManager!.isAllComplete) {
      var temp = Set<int>.from(indices);
      indices.clear();
      await _fileManager?.flushFiles(temp);
    }
    return;
  }

  /// Even if the other peer has choked me, I can still download.
  void _processAllowFast(PeerAllowFast event) {
    var piece = _pieceManager?[event.index];
    if (piece != null && piece.haveAvailableSubPiece()) {
      piece.addAvailablePeer(event.peer);
      _pieceManager?.processDownloadingPiece(event.index);
      requestPieces(event.peer, event.index);
    }
  }

  void _processRejectRequest(PeerRejectEvent event) {
    var piece = _pieceManager?[event.index];
    piece?.pushSubPieceLast(event.begin ~/ DEFAULT_REQUEST_LENGTH);
  }

  void _processPeerDispose(PeerDisposeEvent event) {
    if (_pieceManager == null) return;
    var bufferRequests = event.peer.requestBuffer;

    _pushSubPiecesBack(bufferRequests);
    var completedPieces = event.peer.remoteCompletePieces;
    for (var index in completedPieces) {
      _pieceManager![index]?.removeAvailablePeer(event.peer);
    }
  }

  void _pushSubPiecesBack(List<List<int>> requests) {
    if (requests.isEmpty || _pieceManager == null) return;
    for (var element in requests) {
      var pieceIndex = element[0];
      var begin = element[1];
      // TODO This is dangerous here. Currently, we are dividing a piece into 16 KB chunks. What if it's not the case?
      var piece = _pieceManager![pieceIndex];
      var subindex = begin ~/ DEFAULT_REQUEST_LENGTH;
      piece?.pushSubPiece(subindex);
    }
  }

  void _processReceivePiece(PeerPieceEvent event) {
    if (_pieceManager == null || _peersManager == null) return;

    var piece = _pieceManager![event.index];
    var i = event.index;
    if (piece != null) {
      var blockStart = piece.offset + event.begin;
      var blockEnd = blockStart + event.block.length;
      if (blockEnd > piece.end) {
        _log.info('Error:', 'Piece overlaps with next piece');
        // will request the same piece below
      } else {
        if (!piece.isCompleted) {
          pieceManager?.processReceivedBlock(
              event.index, event.begin, event.block);
        }
        // request available subpiece
        if (piece.haveAvailableSubPiece()) i = -1;
      }
    }

    Timer.run(() => requestPieces(event.peer, i));
  }

  void _processPeerHandshake(PeerHandshakeEvent event) {
    if (_fileManager == null) return;
    event.peer.sendBitfield(_fileManager!.localBitfield);
  }

  void _processPeerRequest(PeerRequestEvent event) {
    if (_fileManager == null ||
        _peersManager == null ||
        _peersManager!.isPaused) return;
    _fileManager!.readFile(event.index, event.begin, event.length);
  }

  void _processHaveAll(PeerHaveAll event) {
    _processBitfieldUpdate(
        PeerBitfieldEvent(event.peer, event.peer.remoteBitfield));
  }

  void _processHaveNone(PeerHaveNone event) {
    _processBitfieldUpdate(PeerBitfieldEvent(event.peer, null));
  }

  void _processBitfieldUpdate(PeerBitfieldEvent bitfieldEvent) {
    if (_fileManager == null) return;
    if (bitfieldEvent.bitfield != null) {
      if (bitfieldEvent.peer.interestedRemote) return;
      if (_fileManager!.isAllComplete && bitfieldEvent.peer.isSeeder) {
        bitfieldEvent.peer.dispose(BadException(
            "Do not connect to Seeder if the download is already completed"));
        return;
      }
      for (var i = 0; i < _fileManager!.piecesNumber; i++) {
        if (bitfieldEvent.bitfield!.getBit(i)) {
          if (!bitfieldEvent.peer.interestedRemote &&
              !_fileManager!.localHave(i)) {
            bitfieldEvent.peer.sendInterested(true);
            return;
          }
        }
      }
    }
    bitfieldEvent.peer.sendInterested(false);
  }

  void _processHaveUpdate(PeerHaveEvent event) {
    if (pieceManager == null || _fileManager == null || _peersManager == null) {
      return;
    }
    var canRequest = false;
    for (var index in event.indices) {
      if (_pieceManager![index] == null) continue;

      if (!_fileManager!.localHave(index)) {
        // if peer is choking us just send interested
        if (event.peer.chokeMe) {
          event.peer.sendInterested(true);
        } else {
          // not choking us, add the peer to the piece and request below
          canRequest = true;
          pieceManager![index]?.addAvailablePeer(event.peer);
        }
      }
    }
    if (canRequest && event.peer.isSleeping) {
      // peer doesn't have requests, so we can request
      Timer.run(() => requestPieces(event.peer));
    }
  }

  void _processChokeChange(PeerChokeChanged event) {
    if (_pieceManager == null || _peersManager == null) return;
    // Update available peers for pieces.
    if (!event.choked) {
      var completedPieces = event.peer.remoteCompletePieces;
      for (var index in completedPieces) {
        _pieceManager![index]?.addAvailablePeer(event.peer);
      }
      // Start requesting
      Timer.run(() => requestPieces(event.peer));
    } else {
      var completedPieces = event.peer.remoteCompletePieces;
      for (var index in completedPieces) {
        _pieceManager![index]?.removeAvailablePeer(event.peer);
      }
    }
  }

  void _processRequestTimeout(Peer peer, List<List<int>> requests) {
    if (_pieceManager == null || _peersManager == null) return;
    var flag = false;
    for (var request in requests) {
      if (request[4] >= 3) {
        flag = true;
        Timer.run(() => peer.requestCancel(request[0], request[1], request[2]));
        var index = request[0];
        var begin = request[1];
        var subindex = begin ~/ DEFAULT_REQUEST_LENGTH;
        var piece = _pieceManager![index];
        piece?.pushSubPiece(subindex);
      }
    }
    // Wake up other possibly idle peers.
    if (flag) {
      for (var p in _peersManager!.activePeers) {
        if (p != peer && p.isSleeping) {
          // TODO: should we request from all peers ?
          Timer.run(() => requestPieces(p));
        }
      }
    }
  }

  void requestPieces(Peer peer, [int pieceIndex = -1]) async {
    if (_pieceManager == null || _peersManager == null) return;
    if (_peersManager!.addPausedRequest(peer, pieceIndex)) return;
    Piece? piece;
    if (pieceIndex != -1) {
      // a specific piece requested
      piece = _pieceManager![pieceIndex];
      // if the piece is available but doesn't have available subpiece,
      // select a different subpiece
      if (piece != null && !piece.haveAvailableSubPiece()) {
        piece = _pieceManager!
            .selectPiece(peer, _pieceManager!, peer.remoteSuggestPieces);
      }
    } else {
      // no specific piece requested, select one
      piece = _pieceManager!
          .selectPiece(peer, _pieceManager!, peer.remoteSuggestPieces);
    }

    // at this point we have a piece that we know is:
    // - available in the peer
    // - have subPieces
    if (piece == null) return;

    var subIndex = piece.popSubPiece();
    if (subIndex == null) return;
    var size = DEFAULT_REQUEST_LENGTH; // Block size is calculated dynamically.
    var begin = subIndex * size;
    if ((begin + size) > piece.byteLength) {
      size = piece.byteLength - begin;
    }
    if (!peer.sendRequest(piece.index, begin, size)) {
      piece.pushSubPiece(subIndex);
    } else {
      Timer.run(() => requestPieces(peer, pieceIndex));
    }
  }

  @override
  Future stop([bool force = false]) async {
    await _tracker?.stop(force);
    await _streamingServer?.stop();
    events.emit(TaskStopped());
    await dispose();
  }

  @override
  Future<void> dispose() async {
    await _flushFiles(_flushIndicesBuffer);
    _flushIndicesBuffer.clear();
    events.dispose();
    _dhtRepeatTimer?.cancel();
    _dhtRepeatTimer = null;
    trackerListener?.dispose();
    fileManagerListener?.dispose();
    peersManagerListener?.dispose();
    lsdListener?.dispose();
    _dhtListener?.dispose();
    // This is in order, first stop the tracker, then stop listening on the server socket and all peers, finally close the file system.
    await _tracker?.dispose();
    _tracker = null;

    await _peersManager?.dispose();
    _peersManager = null;
    _serverSocketListener?.cancel();
    _serverSocketListener = null;
    await _serverSocket?.close();
    _serverSocket = null;
    await _fileManager?.close();
    _fileManager = null;
    await _dht?.stop();
    _dht = null;
    _lsd?.close();
    _lsd = null;
    _peerIds.clear();
    _comingIp.clear();
    _streamingServer?.stop();

    state = TaskState.stopped;
    return;
  }

  @override
  Future<Map<String, dynamic>> getOptions(Uri uri, String infoHash) {
    var map = {
      'downloaded': _stateFile?.downloaded,
      'uploaded': _stateFile?.uploaded,
      'left': _metaInfo.length - _stateFile!.downloaded,
      'numwant': 50,
      'compact': 1,
      'peerId': _peerId,
      'port': _serverSocket?.port
    };
    return Future.value(map);
  }

  @override
  int? get downloaded => _fileManager?.downloaded;

  @override
  double get progress {
    var d = downloaded;
    if (d == null) return 0.0;
    var l = _metaInfo.length;
    return d / l;
  }

  @override
  int get allPeersNumber {
    if (_peersManager != null) {
      return _peersManager!.peersNumber;
    } else {
      return 0;
    }
  }

  @override
  void addDHTNode(Uri url) {
    _dht?.addBootstrapNode(url);
  }

  @override
  int get connectedPeersNumber {
    if (_peersManager != null) {
      return _peersManager!.connectedPeersNumber;
    } else {
      return 0;
    }
  }

  @override
  int get seederNumber {
    if (_peersManager != null) {
      return _peersManager!.seederNumber;
    } else {
      return 0;
    }
  }

  // TODO debug:
  @override
  double get utpDownloadSpeed {
    if (_peersManager == null) return 0.0;
    return _peersManager!.utpDownloadSpeed;
  }

// TODO debug:
  @override
  double get utpUploadSpeed {
    if (_peersManager == null) return 0.0;
    return _peersManager!.utpUploadSpeed;
  }

// TODO debug:
  @override
  int get utpPeerCount {
    if (_peersManager == null) return 0;
    return _peersManager!.utpPeerCount;
  }

  @override
  void startAnnounceUrl(Uri url, Uint8List infoHash) {
    _tracker?.runTracker(url, infoHash);
  }

  @override
  void requestPeersFromDHT() {
    _dht?.requestPeers(String.fromCharCodes(_metaInfo.infoHashBuffer));
  }
}
