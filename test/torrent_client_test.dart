@Timeout(Duration(seconds: 90))

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:dtorrent_common/dtorrent_common.dart';
import 'package:test/test.dart';
import 'package:dtorrent_parser/dtorrent_parser.dart';
import 'package:dtorrent_task/dtorrent_task.dart';
import 'package:path/path.dart' as path;


final testDirectory = path.join(
  Directory.current.path,
  Directory.current.path.endsWith('test') ? '' : 'test',
);
var torrentsPath =
    path.canonicalize(path.join(testDirectory, '..', '..', '..', 'torrents'));

void main() {
  var directory = path.canonicalize(path.join('..', 'tmp'));

  group('Bitfield test - ', () {
    Bitfield? bitfield;
    var pieces = 123; // Do not provide a number that is multiple of 8
    setUp(() {
      bitfield = Bitfield.createEmptyBitfield(pieces);
    });

    test('Init bitfield', () {
      var c = pieces ~/ 8;
      if (c * 8 != pieces) c++;
      print('check buffer length');
      assert(bitfield!.buffer.length == c);
      for (var element in bitfield!.buffer) {
        assert(element == 0);
      }
    });

    test('random set/get test, and check the complete index ', () {
      var t = Random(); // Increase the range.
      var randomIndex = <int>{};
      for (var i = 0; i < pieces; i++) {
        var index = t.nextInt(pieces * 2);
        if (index >= pieces) {
          bitfield!.setBit(index, true);
          assert(bitfield!.getBit(index) == false);
          bitfield!.setBit(index, false);
          assert(bitfield!.getBit(index) == false);
        } else {
          randomIndex.add(index);
          bitfield!.setBit(index, true);
          assert(bitfield!.getBit(index) == true);
        }
      }
      var indexList = randomIndex.toList();
      indexList.sort((a, b) => a - b);

      var list = bitfield!.completedPieces;
      print('Check completed index list...');
      for (var i = 0; i < list.length; i++) {
        indexList.remove(list[i]);
      }
      assert(indexList.isEmpty);

      assert(bitfield!.haveCompletePiece());
      print('Check bitfield value...');
      for (var index in list) {
        assert(bitfield!.getBit(index));
      }
      var tempList = [];
      tempList.addAll(list);
      for (var index in tempList) {
        bitfield!.setBit(index, false);
      }
      print('Clean all bitfield...');
      for (var element in bitfield!.buffer) {
        assert(element == 0);
      }
    });

    test('add/remote complete list', () {
      var t = Random(); //Increase the range.
      var randomIndex = <int>{};
      for (var i = 0; i < pieces; i++) {
        var index = t.nextInt(pieces * 2);
        if (index >= pieces) {
          bitfield!.setBit(index, true);
          assert(bitfield!.getBit(index) == false);
          bitfield!.setBit(index, false);
          assert(bitfield!.getBit(index) == false);
        } else {
          randomIndex.add(index);
          bitfield!.setBit(index, true);
          assert(bitfield!.getBit(index) == true);
        }
      }

      bitfield!.completedPieces;
      bitfield!.setBit(t.nextInt(pieces * 2), true);
      var length = bitfield!.completedPieces.length;
      bitfield!.setBit(bitfield!.completedPieces.last, true);
      assert(length == bitfield!.completedPieces.length);
      bitfield!.setBit(bitfield!.completedPieces.first, true);
      assert(length == bitfield!.completedPieces.length);
      var first = bitfield!.completedPieces.first;
      bitfield!.setBit(bitfield!.completedPieces.first, false);
      assert(!bitfield!.completedPieces.contains(first));
    });
  });

  group('Piece test - ', () {
    test('create sub-pieces', () async {
      // Simulate bitfields that are divisible.
      var totalSize = 163840;
      var remain = Random().nextInt(100);
      totalSize = totalSize + remain;
      var p = Piece('aaaaaaa', 0, totalSize, 0);
      var size = DEFAULT_REQUEST_LENGTH;
      var subIndex = p.popSubPiece();
      subIndex = p.popLastSubPiece();
      assert(subIndex != null);
      var begin = subIndex! * size;
      if ((begin + size) > p.byteLength) {
        size = p.byteLength - begin;
        assert(remain == size);
      }
    });

    test('piece length less than 16kb', () {
      var p = Piece('aaaaaaa', 0, DEFAULT_REQUEST_LENGTH - 100, 0);
      var l = p.availableSubPieceCount;
      assert(l == 1);
      var sp = p.popSubPiece();
      assert(sp == 0);
      assert(!p.haveAvailableSubPiece());
    });
  });

  group('test same piece find - ', () {
    var pieces = 123; // Do not provide a number that is multiple of 8
    List<Bitfield> bitfieldList = [];
    // Simulate bitfields of multiple peers.
    setUp(() {
      bitfieldList =
          List.generate(10, (index) => Bitfield.createEmptyBitfield(pieces));

      for (var bitfield in bitfieldList) {
        var t = Random();
        var randomIndex = <int>{};
        for (var i = 0; i < pieces; i++) {
          var index = t.nextInt(pieces);
          if (index >= pieces) {
            bitfield.setBit(index, true);
            assert(bitfield.getBit(index) == false);
            bitfield.setBit(index, false);
            assert(bitfield.getBit(index) == false);
          } else {
            randomIndex.add(index);
            bitfield.setBit(index, true);
            assert(bitfield.getBit(index) == true);
          }
        }
      }
    });

    test('ramdon pieces', () {
      for (var element in bitfieldList) {
        print(element.toString());
      }
    });
  });

  group('StateFile Test - ', () {
    Torrent? torrent;
    setUpAll(() async {
      torrent = await Torrent.parse(
          path.join(torrentsPath, 'big-buck-bunny.torrent'));
      var f = File(path.join(directory, '${torrent!.infoHash}.bt.state'));
      if (await f.exists()) await f.delete();
    });
    test('Write/Read StateFile', () async {
      var stateFile = await StateFile.getStateFile(directory, torrent!);
      var b = torrent!.pieces.length ~/ 8;
      if (b * 8 != torrent!.pieces.length) b++;
      assert(stateFile.bitfield.length == b);
      assert(stateFile.bitfield.piecesNum == torrent!.pieces.length);
      assert(!stateFile.bitfield.haveCompletePiece());

      await stateFile.close();
      // To test reading the contents of an empty file after creating it.
      stateFile = await StateFile.getStateFile(directory, torrent!);
      b = torrent!.pieces.length ~/ 8;
      if (b * 8 != torrent!.pieces.length) b++;
      assert(stateFile.bitfield.length == b);
      assert(stateFile.bitfield.piecesNum == torrent!.pieces.length);
      assert(!stateFile.bitfield.haveCompletePiece());
      assert(stateFile.downloaded == 0);
      assert(stateFile.uploaded == 0);

      var updatePiece = <int>{};
      for (var i = 0; i < stateFile.bitfield.piecesNum ~/ 4; i++) {
        var index = randomInt(stateFile.bitfield.piecesNum);
        updatePiece.add(index);
        await stateFile.updateBitfield(index, true);
      }
      await stateFile.updateBitfield(stateFile.bitfield.piecesNum - 1);
      updatePiece.add(stateFile.bitfield.piecesNum - 1);
      var updateList = updatePiece.toList();
      updateList.sort((a, b) => a - b);
      var list = stateFile.bitfield.completedPieces;
      assert(list.length == updateList.length);
      await stateFile.updateUploaded(987654321);
      assert(stateFile.uploaded == 987654321);

      await stateFile.close();
      await stateFile.close(); // What will happen if closed twice?
      var f = File(path.join(directory, '${torrent!.infoHash}.bt.state'));
      var locker = Completer();
      var data = <int>[];
      f.openRead().listen((event) {
        data.addAll(event);
      }, onDone: () {
        var nb = Bitfield.copyFrom(
            stateFile.bitfield.piecesNum, stateFile.bitfield.buffer);
        for (var i = 0; i < nb.completedPieces.length; i++) {
          assert(nb.completedPieces[i] == updateList[i]);
        }
        var v = ByteData.view(Uint8List.fromList(data).buffer);
        var offset = stateFile.bitfield.buffer.length;
        var re = v.getUint64(offset);
        assert(re == stateFile.uploaded);
        locker.complete();
      }, onError: (e) => locker.complete());
      await locker.future;

      stateFile = await StateFile.getStateFile(directory, torrent!);
      b = torrent!.pieces.length ~/ 8;
      if (b * 8 != torrent!.pieces.length) b++;
      assert(stateFile.bitfield.length == b);
      var sd = stateFile.bitfield.completedPieces.length * torrent!.pieceLength;
      sd = sd - (torrent!.pieceLength - torrent!.lastPieceLength);
      assert(stateFile.downloaded == sd);
      print('download: $sd');
      assert(stateFile.uploaded == 987654321);

      for (var i = 0; i < stateFile.bitfield.completedPieces.length; i++) {
        assert(stateFile.bitfield.completedPieces[i] == updateList[i]);
      }
    });

    test('Delete StateFile', () async {
      var stateFile = await StateFile.getStateFile(directory, torrent!);
      var t = File(path.join(directory, '${torrent!.infoHash}.bt.state'));
      assert(await t.exists());
      await stateFile.delete();
      await stateFile.delete(); //Deleting twice.
      assert(!await t.exists());
    });
  });

  group('Temp file access - ', () {
    test('Create/Read/Write/Delete', () async {
      var content = 'DART-TORRENT-CLIENT';
      var buffer = utf8.encode(content);
      var file =
          DownloadFile('test/test.txt', 0, buffer.length, 'test/test.txt', []);
      assert(await file.requestWrite(0, buffer, 0, buffer.length));
      var bytes = await file.requestRead(0, buffer.length);
      var result = String.fromCharCodes(bytes);
      assert(result == content, 'Validating reading content error.');
      await file.close();
      await file.close(); // Closing twice.
      var file1 = File('test/test.txt');
      assert(await file1.exists());
      var b = <int>[];
      var lock = Completer();
      file1.openRead().listen((data) {
        b.addAll(data);
      }, onDone: () {
        var result = String.fromCharCodes(b);
        assert(result == content, 'File content verification error.');
        lock.complete();
      });
      await lock.future;
      await file.delete();
      file1 = File('test/test.txt');
      assert(!await file1.exists(), 'File deletion error.');
    });
  });

  group('Task Tests', () {
    Torrent? torrent;
    setUpAll(() async {
      torrent = await Torrent.parse(
          path.join(torrentsPath, 'big-buck-bunny.torrent'));
      var f = File(path.join(directory, '${torrent!.infoHash}.bt.state'));
      if (await f.exists()) await f.delete();
    });

    test('Stop task test', () async {
      assert(torrent != null, 'No torrent provided');
      final newTask = TorrentTask.newTask(torrent!, directory);
      await newTask.start();
      assert(newTask.state == TaskState.running);
      await newTask.stop();
      assert(newTask.state == TaskState.stopped);
      await newTask.dispose();
    });
    test('TaskFileCompleted test', () async {
      assert(torrent != null, 'No torrent provided');
      var countTaskFileCompleted = 0;
      final newTask = TorrentTask.newTask(torrent!, directory);
      final completer = Completer();
      newTask.createListener().listen(
            (event) {
          switch (event.runtimeType) {
            case TaskCompleted:
              print('TaskCompleted');
              completer.complete();
              newTask.stop();
              return;
            case TaskFileCompleted:
              countTaskFileCompleted++;
              return;
            case StateFileUpdated:
              print('Downloaded: ${(newTask.progress * 100).round()}%');
              return;
            default:
          }
        },
      );
      await newTask.start();
      await completer.future;
      await newTask.stop();
      final pathToFile = path.join(directory, '${torrent?.files.first.name}');
      final file = File(pathToFile);
      if (await file.exists()) {
        await file.delete();
      }
      await newTask.dispose();
      assert(countTaskFileCompleted == torrent!.files.length, 'File can be completed only once');
    });
  });
}
