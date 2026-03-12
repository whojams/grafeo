import 'package:grafeo/grafeo.dart';

void main() async {
  print('Testing Grafeo issue #133');

  // Test 1: Open an in-memory database
  print('1. Opening local storage');
  final db = await GrafeoDatabase.openMemory();

  // query db [info](https://grafeo.dev/api/node/database/?h=schema#info)
  print(await db.execute('RETURN info() as db_info'));

  // query db [schema](https://grafeo.dev/api/node/database/?h=schema#schema)
  print(await db.execute('RETURN schema() as db_schema'));

  // create a empty schema to be switch.
  await db.execute('CREATE SCHEMA IF NOT EXISTS default');
  print('2. ✓ Created a empty schema to be switch.');

  // create a data container schema
  await db.execute('CREATE GRAPH IF NOT EXISTS reporting');
  print('3. ✓ CREATE GRAPH IF NOT EXISTS reporting');
  // switch to data container
  await db.execute('SESSION  SET SCHEMA reporting');
  print('4. ✓ SESSION  SET SCHEMA reporting, (actually, reporting is a graph)');
  await db.execute("INSERT (:Person {name: 'Kenny'})");
  print('5. ✓ INSERT (:Person {name: \'Kenny\'})');

  print('6. Getting persons');
  var persons = await db.executeWithParams('MATCH (n:Person) RETURN n', {});
  print('✓ Persons: $persons');

  await db.execute('SESSION SET SCHEMA default');
  print('7. ✓ SESSION SET SCHEMA default');
  print('8. Getting persons');
  persons = await db.executeWithParams('MATCH (n:Person) RETURN n', {});
  print('✓ Persons: $persons');

  // Test 6: Close the database
  print('6. Closing database...');
  await db.close();
  print('✓ Database closed successfully');

  print('All tests passed!');
}
