import 'package:grafeo/grafeo.dart';
import 'package:test/test.dart';

late GrafeoDatabase db;

main() {
  setUpAll(() async {
    db = await GrafeoDatabase.openMemory();
  });

  group('Test the executability of the database method: ', () {
    test('Test info', () async {
      var info = await db.info();
      print('db info: $info');
    });

    test("Test nodeCount", () async {
      var nodeCount = await db.nodeCount();
      print('node count: $nodeCount');
    });

    test("Test edgeCount", () async {
      var edgeCount = await db.edgeCount();
      print('edge count: $edgeCount');
    });

    test('Test version', () async {
      var version = GrafeoDatabase.version();
      print('version: $version');
    });
  });

  tearDownAll(() async {
    await db.close();
  });
}
