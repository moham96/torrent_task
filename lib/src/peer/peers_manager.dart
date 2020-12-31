import 'dart:async';
import 'dart:developer';
import 'dart:io';
import 'dart:typed_data';

import 'package:torrent_model/torrent_model.dart';

import 'bitfield.dart';
import 'peer.dart';
import '../file/download_file_manager.dart';
import '../piece/piece_manager.dart';
import '../piece/piece.dart';
import '../piece/piece_provider.dart';
import '../utils.dart';

const MAX_ACTIVE_PEERS = 50;

const MAX_WRITE_BUFFER_SIZE = 10 * 1024 * 1024;

const MAX_UPLOADED_NOTIFY_SIZE = 1024 * 1024 * 10; // 10 mb

///
/// TODO:
/// - 没有处理对外的Suggest Piece/Fast Allow
class PeersManager {
  bool _disposed = false;

  bool get isDisposed => _disposed;

  final Set<Peer> _peers = {};

  final Set<Uri> _lastUTPEX = {};

  String localExtenelIP;

  /// 写入磁盘的缓存最大值
  int maxWriteBufferSize;

  final _flushIndicesBuffer = <int>{};

  final Set<void Function(Uri)> _newPeerFoundHandler = {};

  final Set<void Function()> _allcompletehandles = {};

  final Set<void Function()> _noActivePeerhandles = {};

  final List<List<dynamic>> _timeoutRequest = [];

  final Torrent _metaInfo;

  int _uploaded = 0;

  int _uploadedNotifySize = 0;

  final List<List> _remoteRequest = [];

  final Map<String, int> _remoteRequestCounts = {};

  final DownloadFileManager _fileManager;

  final PieceProvider _pieceProvider;

  final PieceManager _pieceManager;

  bool _paused = false;

  Timer _keepAliveTimer;

  final List _pausedRequest = [];

  final Map<String, List> _pausedRemoteRequest = {};

  Timer _ut_pex_timer;

  PeersManager(this._pieceManager, this._pieceProvider, this._fileManager,
      this._metaInfo,
      [this.maxWriteBufferSize = MAX_WRITE_BUFFER_SIZE]) {
    assert(_pieceManager != null &&
        _pieceProvider != null &&
        _fileManager != null);
    // hook FileManager and PieceManager
    _fileManager.onSubPieceWriteComplete(_processSubPieceWriteComplte);
    _fileManager.onSubPieceReadComplete(readSubPieceComplete);
    _pieceManager.onPieceComplete(_processPieceWriteComplete);

    _ut_pex_timer = Timer.periodic(Duration(seconds: 60), (timer) {
      _sendUt_pex_peers();
    });
  }

  bool get isPaused => _paused;

  int get peersNumber {
    if (_peers == null || _peers.isEmpty) return 0;
    return _peers.length;
  }

  double get downloadSpeed {
    var s = 0.0;
    if (_peers != null) {
      s = _peers.fold(
          0.0, (previousValue, peer) => previousValue + peer.downloadSpeed);
    }
    return s;
  }

  double get uploadSpeed {
    var s = 0.0;
    if (_peers != null) {
      s = _peers.fold(
          0.0, (previousValue, peer) => previousValue + peer.uploadSpeed);
    }
    return s;
  }

  void hookPeer(Peer peer) {
    if (peer.address.host == localExtenelIP) return;
    if (_peerExsist(peer)) return;
    peer.onDispose(_processPeerDispose);
    peer.onBitfield(_processBitfieldUpdate);
    peer.onHaveAll(_processHaveAll);
    peer.onHaveNone(_processHaveNone);
    peer.onHandShake(_processPeerHandshake);
    peer.onChokeChange(_processChokeChange);
    peer.onInterestedChange(_processInterestedChange);
    peer.onConnect(_peerConnected);
    peer.onHave(_processHaveUpdate);
    peer.onPiece(_processReceivePiece);
    peer.onRequest(_processRemoteRequest);
    peer.onRequestTimeout(_processRequestTimeout);
    peer.onRejectRequest(_processRejectRequest);
    peer.onAllowFast(_processAllowFast);
    peer.onExtendedEvent(_processExtendedMessage);
    _registerExtended(peer);
    peer.connect();
  }

  /// 支持哪些扩展在这里添加
  void _registerExtended(Peer peer) {
    peer.registerExtened('ut_pex');
  }

  void unHookPeer(Peer peer) {
    if (peer == null) return;
    peer.offDispose(_processPeerDispose);
    peer.offBitfield(_processBitfieldUpdate);
    peer.offHaveAll(_processHaveAll);
    peer.offHaveNone(_processHaveNone);
    peer.offHandShake(_processPeerHandshake);
    peer.offChokeChange(_processChokeChange);
    peer.offInterestedChange(_processInterestedChange);
    peer.offConnect(_peerConnected);
    peer.offHave(_processHaveUpdate);
    peer.offPiece(_processReceivePiece);
    peer.offRequest(_processRemoteRequest);
    peer.offRequestTimeout(_processRequestTimeout);
    peer.offRejectRequest(_processRejectRequest);
    peer.offAllowFast(_processAllowFast);
    peer.offExtendedEvent(_processExtendedMessage);
  }

  bool _peerExsist(Peer id) {
    return _peers.contains(id);
  }

  void _processExtendedMessage(dynamic source, String name, dynamic data) {
    if (name == 'ut_pex') {
      var added = data['added'] as List;
      for (var i = 0; i < added.length; i += 6) {
        var uri = parseAddress(added, i);
        if (uri.host == localExtenelIP) continue;
        print('获得一个PEX：$uri');
        Timer.run(() => _fireNewPeerFound(uri));
      }
    }
    if (name == 'handshake') {
      localExtenelIP = InternetAddress.fromRawAddress(data['yourip']).host;
    }
  }

  void _sendUt_pex_peers() {
    var dropped = <Uri>[];
    var added = <Uri>[];
    _peers.forEach((p) {
      if (!_lastUTPEX.remove(p.address)) {
        added.add(p.address);
      }
    });
    _lastUTPEX.forEach((element) {
      dropped.add(element);
    });
    _lastUTPEX.clear();

    var data = {};
    data['added'] = [];
    added.forEach((element) {
      _lastUTPEX.add(element);
      var p = Uint8List(2);
      ByteData.view(p.buffer).setUint16(0, element.port);
      var ip = InternetAddress.tryParse(element.host);
      if (ip != null) {
        var b = <int>[];
        b.addAll(ip.rawAddress);
        b.addAll(p);
        data['added'].addAll(b);
      }
    });
    data['dropped'] = [];
    dropped.forEach((element) {
      var p = Uint8List(2);
      ByteData.view(p.buffer).setUint16(0, element.port);
      var ip = InternetAddress.tryParse(element.host);
      if (ip != null) {
        var b = <int>[];
        b.addAll(ip.rawAddress);
        b.addAll(p);
        data['dropped'].addAll(b);
      }
    });
    if (data['added'].isEmpty && data['dropped'].isEmpty) return;
    _peers.forEach((peer) {
      peer.sendExtendMessage('ut_pex', data);
    });
  }

  void _processSubPieceWriteComplte(int pieceIndex, int begin, int length) {
    _pieceManager.processSubPieceWriteComplete(pieceIndex, begin, length);
  }

  void _processPieceWriteComplete(int index) async {
    if (_fileManager.localHave(index)) return;
    await _fileManager.updateBitfield(index);
    _peers.forEach((peer) {
      // if (!peer.remoteHave(index)) {
      peer.sendHave(index);
      // }
    });
    _flushIndicesBuffer.add(index);
    if (_fileManager.isAllComplete) {
      await _flushFiles(_flushIndicesBuffer);
      _fireAllComplete();
    } else {
      await _flushFiles(_flushIndicesBuffer);
    }
  }

  Future _flushFiles(final Set<int> indices) async {
    if (indices.isEmpty) return;
    var piecesSize = _metaInfo.pieceLength;
    var _buffer = indices.length * piecesSize;
    if (_buffer >= maxWriteBufferSize || _fileManager.isAllComplete) {
      var temp = Set<int>.from(indices);
      indices.clear();
      await _fileManager.flushFiles(temp);
    }
    return;
  }

  void _fireAllComplete() {
    _allcompletehandles.forEach((element) {
      Timer.run(() => element());
    });
  }

  bool onAllComplete(void Function() h) {
    return _allcompletehandles.add(h);
  }

  bool offAllComplete(void Function() h) {
    return _allcompletehandles.remove(h);
  }

  bool onNewPeerFound(void Function(Uri uri) h) {
    return _newPeerFoundHandler.add(h);
  }

  bool offNewPeerFound(void Function(Uri uri) h) {
    return _newPeerFoundHandler.remove(h);
  }

  void _fireNewPeerFound(Uri uri) {
    _newPeerFoundHandler.forEach((element) {
      element(uri);
    });
  }

  void readSubPieceComplete(int pieceIndex, int begin, List<int> block) {
    var dindex = [];
    for (var i = 0; i < _remoteRequest.length; i++) {
      var request = _remoteRequest[i];
      if (request[0] == pieceIndex && request[1] == begin) {
        dindex.add(i);
        var peer = request[2] as Peer;
        _remoteRequestCounts[peer.id]--;
        if (peer != null && !peer.isDisposed) {
          if (peer.sendPiece(pieceIndex, begin, block)) {
            _uploaded += block.length;
            _uploadedNotifySize += block.length;
          }
        }
        break;
      }
    }
    if (dindex.isNotEmpty) {
      dindex.forEach((i) {
        _remoteRequest.removeAt(i);
      });
      if (_uploadedNotifySize >= MAX_UPLOADED_NOTIFY_SIZE) {
        _uploadedNotifySize = 0;
        _fileManager.updateUpload(_uploaded);
      }
    }
  }

  /// 即使对方choke了我，也可以下载
  void _processAllowFast(dynamic source, int index) {
    var peer = source as Peer;
    var piece = _pieceProvider[index];
    if (piece != null && piece.haveAvalidateSubPiece()) {
      _pieceManager.processDownloadingPiece(
          peer.id, index, peer.remoteBitfield.completedPieces);
      _requestPieces(source, index);
    }
  }

  void _processSuggestPiece(dynamic source, int index) {}

  void _processRejectRequest(dynamic source, int index, int begin, int length) {
    var piece = _pieceProvider[index];
    piece?.pushSubPieceLast(begin ~/ DEFAULT_REQUEST_LENGTH);
  }

  void _processPeerDispose(dynamic source, [dynamic reason]) {
    var peer = source as Peer;
    var bufferRequests = peer.requestBuffer;
    // log('Peer已销毁, ${peer.address},退还收到未收到Request:$bufferRequests,将其删除',
    //     error: reason, name: runtimeType.toString());

    bufferRequests.forEach((element) {
      var pindex = element[0];
      var begin = element[1];
      var length = element[2];
      var piece = _pieceManager[pindex];
      var subindex = begin ~/ DEFAULT_REQUEST_LENGTH;
      _removeTimeoutRequest(pindex, begin, length);
      piece?.pushSubPiece(subindex);
    });
    var completedPieces = peer.remoteCompletePieces;
    completedPieces.forEach((index) {
      _pieceProvider[index]?.removeAvalidatePeer(peer.id);
    });
    _peers.remove(peer);
    _pausedRemoteRequest.remove(peer.id);
    _remoteRequestCounts.remove(peer.id);
    var tempIndex = [];
    for (var i = 0; i < _pausedRequest.length; i++) {
      var pr = _pausedRequest[i];
      if (pr[0] == peer) {
        tempIndex.add(i);
      }
    }
    tempIndex.forEach((index) {
      _pausedRequest.removeAt(index);
    });
    if (_peers.isEmpty) {
      _fireNoActivePeer();
    }
  }

  bool onNoActivePeerEvent(Function k) {
    return _noActivePeerhandles.add(k);
  }

  bool offNoActivePeerEvent(Function k) {
    return _noActivePeerhandles.remove(k);
  }

  void _fireNoActivePeer() {
    _noActivePeerhandles.forEach((element) {
      Timer.run(() => element());
    });
  }

  void _peerConnected(dynamic source) {
    var peer = source as Peer;
    log('${peer.address} is connected', name: runtimeType.toString());
    _peers.add(peer);
    peer.sendHandShake();
  }

  void _requestPieces(dynamic source, [int pieceIndex = -1]) {
    if (isPaused) {
      _pausedRequest.add([source, pieceIndex]);
      return;
    }
    var peer = source as Peer;
    Piece piece;
    if (pieceIndex != -1) {
      piece = _pieceProvider[pieceIndex];
    } else {
      piece = _pieceManager.selectPiece(peer.id, peer.remoteCompletePieces,
          _pieceProvider, peer.remoteSuggestPieces);
    }
    if (piece == null) {
      if (_timeoutRequest.isNotEmpty) {
        // 如果已经没有可以请求的piece，看看超时piece
        var timeoutR = _timeoutRequest.removeAt(0);
        var p = timeoutR[3] as Peer;
        if (p != null) {
          p.removeRequest(timeoutR[0], timeoutR[1], timeoutR[2]);
        }
        if (!peer.sendRequest(timeoutR[0], timeoutR[1], timeoutR[2])) {
          _timeoutRequest.insert(0, timeoutR);
        }
      }
      return;
    }
    var subIndex = piece.popSubPiece();
    var size = DEFAULT_REQUEST_LENGTH; // block大小现算
    var begin = subIndex * size;
    if ((begin + size) > piece.byteLength) {
      size = piece.byteLength - begin;
    }

    if (!peer.sendRequest(piece.index, begin, size)) {
      piece.pushSubPiece(subIndex);
    }
  }

  void _processReceivePiece(
      dynamic source, int index, int begin, List<int> block) {
    var peer = source as Peer;
    var rindex = -1;
    for (var i = 0; i < _timeoutRequest.length; i++) {
      var tr = _timeoutRequest[i];
      if (tr[0] == index && tr[1] == begin && tr[2] == block.length) {
        log('超时Request[$index,$begin]已从${peer.address}获得，当前超时Request:$_timeoutRequest',
            name: runtimeType.toString());
        rindex = i;
        break;
      }
    }
    if (rindex != -1) {
      var tr = _timeoutRequest.removeAt(rindex);
      var peer = tr[3] as Peer;
      if (peer != null && !peer.isDisposed) {
        peer.removeRequest(index, begin, block.length);
      }
    }
    _fileManager.writeFile(index, begin, block);
    var nextIndex = _pieceManager.selectPieceWhenReceiveData(
        peer.id, peer.remoteCompletePieces, index, begin);
    _requestPieces(peer, nextIndex);
  }

  void _processPeerHandshake(dynamic source, String remotePeerId, data) {
    var peer = source as Peer;
    peer.sendBitfield(_fileManager.localBitfield);
  }

  void _processRemoteRequest(dynamic source, int index, int begin, int length) {
    if (isPaused) {
      var peer = source as Peer;
      var pausedRequest = _pausedRemoteRequest[peer.id];
      if (pausedRequest == null) {
        pausedRequest = [];
        _pausedRemoteRequest[peer.id] = pausedRequest;
      }
      if (pausedRequest.length <= 6) {
        pausedRequest.add([source, index, begin, length]);
      } else {
        peer.dispose('too many requests');
      }
      return;
    }
    var peer = source as Peer;
    var count = _remoteRequestCounts[peer.id];
    if (count == null) _remoteRequestCounts[peer.id] = 0;
    if (_remoteRequestCounts[peer.id] >= 6) {
      peer.dispose('too many requests');
      return;
    }
    _remoteRequestCounts[peer.id]++;
    _remoteRequest.add([index, begin, peer]);
    _fileManager.readFile(index, begin, length);
  }

  void _processHaveAll(dynamic source) {
    var peer = source as Peer;
    _processBitfieldUpdate(source, peer.remoteBitfield);
  }

  void _processHaveNone(dynamic source) {
    _processBitfieldUpdate(source, null);
  }

  void _processBitfieldUpdate(dynamic source, Bitfield bitfield) {
    var peer = source as Peer;
    if (bitfield != null) {
      if (peer.interestedRemote) return;
      for (var i = 0; i < _fileManager.piecesNumber; i++) {
        if (bitfield.getBit(i)) {
          if (!peer.interestedRemote && !_fileManager.localHave(i)) {
            peer.sendInterested(true);
            return;
          }
        }
      }
    }
    log('${peer.address} 没有我要的资源 $bitfield，发送 not interested');
    peer.sendInterested(false);
  }

  void _processHaveUpdate(dynamic source, int index) {
    var peer = source as Peer;
    if (!_fileManager.localHave(index)) {
      peer.sendInterested(true);
      _pieceProvider[index]?.addAvalidatePeer(peer.id);
      Timer.run(() => _requestPieces(peer));
    }
  }

  void _processChokeChange(dynamic source, bool choke) {
    var peer = source as Peer;
    // 更新pieces的可用Peer
    if (!choke) {
      var completedPieces = peer.remoteCompletePieces;
      completedPieces.forEach((index) {
        _pieceProvider[index]?.addAvalidatePeer(peer.id);
      });
      // 这里开始通知request;
      Timer.run(() => _requestPieces(peer));
    } else {
      var completedPieces = peer.remoteCompletePieces;
      completedPieces.forEach((index) {
        _pieceProvider[index]?.removeAvalidatePeer(peer.id);
      });
    }
  }

  void _processInterestedChange(dynamic source, bool interested) {
    var peer = source as Peer;
    if (interested) {
      peer.sendChoke(false);
    } else {
      peer.sendChoke(true); // 不感兴趣就choke它
    }
  }

  void _processRequestTimeout(
      dynamic source, int index, int begin, int length) {
    var peer = source as Peer;
    // 如果超时，不会重新请求，等待。实在不来会在销毁peer的时候释放所有它对应的请求
    _addTimeoutRequest(index, begin, length, source);
    log('从 ${peer.address} 请求 [$index,$begin] 超时 , 所有超时Request :$_timeoutRequest',
        name: runtimeType.toString());
    // if (_pieceProvider[index] != null &&
    //     _pieceProvider[index].haveAvalidateSubPiece()) {
    //   _requestPieces(peer, index);
    // } else {
    //   _requestPieces(peer);
    // }
  }

  /// 往time out request buffer中记录
  bool _addTimeoutRequest(int index, int begin, int length, Peer peer) {
    for (var i = 0; i < _timeoutRequest.length; i++) {
      var r = _timeoutRequest[i];
      if (r[0] == index && r[1] == begin && length == r[2]) {
        return false;
      }
    }
    _timeoutRequest.add([index, begin, length, peer]);
    return true;
  }

  bool _removeTimeoutRequest(int index, int begin, int length) {
    var di;
    for (var i = 0; i < _timeoutRequest.length; i++) {
      var r = _timeoutRequest[i];
      if (r[0] == index && r[1] == begin && r[2] == length) {
        di = i;
        break;
      }
    }
    if (di != null) {
      _timeoutRequest.removeAt(di);
      return true;
    }
    return false;
  }

  void _sendKeepAliveToAll() {
    _peers?.forEach((peer) {
      Timer.run(() => _keepAlive(peer));
    });
  }

  void _keepAlive(Peer peer) {
    peer.sendKeeplive();
  }

  void pause() {
    if (_paused) return;
    _paused = true;
    _keepAliveTimer?.cancel();
    _keepAliveTimer = Timer(Duration(seconds: 110), _sendKeepAliveToAll);
  }

  void resume() {
    if (!_paused) return;
    _paused = false;
    _keepAliveTimer?.cancel();
    _keepAliveTimer = null;
    _pausedRequest.forEach((element) {
      var peer = element[0] as Peer;
      var index = element[1];
      if (!peer.isDisposed) Timer.run(() => _requestPieces(peer, index));
    });
    _pausedRequest.clear();

    _pausedRemoteRequest.forEach((key, value) {
      value.forEach((element) {
        var peer = element[0] as Peer;
        var index = element[1];
        var begin = element[2];
        var length = element[3];
        if (!peer.isDisposed) {
          Timer.run(() => _processRemoteRequest(peer, index, begin, length));
        }
      });
    });
    _pausedRemoteRequest.clear();
  }

  Future disposeAllSeeder([dynamic reason]) async {
    _peers?.forEach((peer) async {
      if (peer.isSeeder) {
        await peer.dispose(reason);
      }
    });
    return;
  }

  Future dispose() async {
    if (isDisposed) return;
    _disposed = true;
    _ut_pex_timer?.cancel();
    _ut_pex_timer = null;

    _fileManager.offSubPieceWriteComplete(_processSubPieceWriteComplte);
    _fileManager.offSubPieceReadComplete(readSubPieceComplete);
    _pieceManager.offPieceComplete(_processPieceWriteComplete);

    // await _fileManager.flushPiece(_flushBuffer.toList());
    await _flushFiles(_flushIndicesBuffer);
    _flushIndicesBuffer?.clear();
    _allcompletehandles?.clear();
    _noActivePeerhandles?.clear();
    _timeoutRequest?.clear();
    _remoteRequest?.clear();
    _pausedRequest.clear();
    _pausedRemoteRequest.clear();
    _remoteRequestCounts.clear();
    _newPeerFoundHandler.clear();
    Function _disposePeers = (Set<Peer> peers) async {
      if (peers != null && peers.isNotEmpty) {
        for (var i = 0; i < peers.length; i++) {
          var peer = peers.elementAt(i);
          unHookPeer(peer);
          await peer.dispose('Peer Manager disposed');
        }
      }
      peers.clear();
    };
    await _disposePeers(_peers);

    _timeoutRequest.clear();
  }
}