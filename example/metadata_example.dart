import 'dart:async';
import 'dart:typed_data';

import 'package:b_encode_decode/b_encode_decode.dart';
import 'package:dtorrent_common/dtorrent_common.dart';
import 'package:dtorrent_parser/dtorrent_parser.dart';
import 'package:dtorrent_task/src/metadata/metadata_downloader.dart';
import 'package:dtorrent_task/dtorrent_task.dart';
import 'package:dtorrent_task/src/task_events.dart';
import 'package:dtorrent_tracker/dtorrent_tracker.dart';
import 'package:events_emitter2/events_emitter2.dart';

void main(List<String> args) async {
  var infohashString = '217bddb5816f2abc56ce1d9fe430711542b109cc';
  var metadata = MetadataDownloader(infohashString);
  // Metadata download contains a DHT , it will search the peer via DHT,
  // but it's too slow , sometimes DHT can not find any peers
  metadata.startDownload();
  // so for this example , I use the public trackers to help MetaData download to search Peer nodes:
  var tracker = TorrentAnnounceTracker(metadata);
  var trackerListener = tracker.createListener();

  // When metadata contents download complete , it will send this event and stop itself:
  metadata.onDownloadComplete((data) async {
    tracker.stop(true);
    var msg = decode(Uint8List.fromList(data));
    Map<String, dynamic> torrent = {};
    torrent['info'] = msg;
    var torrentModel = parseTorrentFileContent(torrent);
    if (torrentModel != null) {
      print('complete , info : ${torrentModel.name}');
      var startTime = DateTime.now().millisecondsSinceEpoch;
      var task = TorrentTask.newTask(torrentModel, 'tmp');
      Timer? timer;
      EventsListener<TaskEvent> listener = task.createListener();
      listener
        ..on<TaskCompleted>((event) {
          print(
              'Complete! spend time : ${((DateTime.now().millisecondsSinceEpoch - startTime) / 60000).toStringAsFixed(2)} minutes');
          timer?.cancel();
          task.stop();
        })
        ..on<TaskStopped>(((event) {
          print('Task Stopped');
        }));

      timer = Timer.periodic(Duration(seconds: 2), (timer) async {
        var progress = '${(task.progress * 100).toStringAsFixed(2)}%';
        var ads =
            ((task.averageDownloadSpeed) * 1000 / 1024).toStringAsFixed(2);
        var aps = ((task.averageUploadSpeed) * 1000 / 1024).toStringAsFixed(2);
        var ds = ((task.currentDownloadSpeed) * 1000 / 1024).toStringAsFixed(2);
        var ps = ((task.uploadSpeed) * 1000 / 1024).toStringAsFixed(2);

        var utpDownloadSpeed =
            ((task.utpDownloadSpeed) * 1000 / 1024).toStringAsFixed(2);
        var utpUploadSpeed =
            ((task.utpUploadSpeed) * 1000 / 1024).toStringAsFixed(2);
        var utpPeerCount = task.utpPeerCount;

        var active = task.connectedPeersNumber;
        var seeders = task.seederNumber;
        var all = task.allPeersNumber;
        print(
            'Progress : $progress , Peers:($active/$seeders/$all)($utpPeerCount) . Download speed : ($utpDownloadSpeed)($ads/$ds)kb/s , upload speed : ($utpUploadSpeed)($aps/$ps)kb/s');
      });
      await task.start();
    }
  });

  var u8List = Uint8List.fromList(metadata.infoHashBuffer);
  trackerListener.on<AnnouncePeerEventEvent>((event) {
    if (event.event == null) return;
    var peers = event.event!.peers;
    for (var element in peers) {
      metadata.addNewPeerAddress(element, PeerSource.tracker);
    }
  });
  findPublicTrackers().listen((announceUrls) {
    for (var element in announceUrls) {
      tracker.runTracker(element, u8List);
    }
  });
}
