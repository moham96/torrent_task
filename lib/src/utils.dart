import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

String generatePeerId([String prefix = '-bDRLIN-']) {
  var r = randomBytes(9);
  var base64Str = base64Encode(r);
  var id = prefix + base64Str;
  return id;
}

Uint8List randomBytes(count) {
  var random = Random();
  var bytes = Uint8List(count);
  for (var i = 0; i < count; i++) {
    bytes[i] = random.nextInt(254);
  }
  return bytes;
}

/// return random int number , `0 - max`
///
/// [max] values  between 1 and (1<<32) inclusive.
int randomInt(int max) {
  return Random(DateTime.now().millisecond).nextInt(max);
}

Random createRandom() {
  return Random(DateTime.now().millisecond);
}

/// pow(2, 14)
///
/// download piece max size
const DEFAULT_REQUEST_LENGTH = 16384;

/// pow(2,17)
///
/// Remote is request piece length large or eqaul this length
/// , it must close the connection
const MAX_REQUEST_LENGTH = 131072;