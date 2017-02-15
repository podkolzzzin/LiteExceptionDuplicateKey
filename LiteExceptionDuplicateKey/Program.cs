using LiteDB;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LiteExceptionDuplicateKey {

  class Program {

    static string[] lines = File.ReadAllLines("AdventureWorks2016CTP3 20170215 1234.sql");
    static int count;

    static void Main(string[] args) {

      string objectId = ObjectId.NewObjectId().ToString();

      while (true) {
        Write(objectId);
      }
    }

    static void Write(string objectId) {
      count++;
      Console.WriteLine($"Start write file #{count}");
      using (var db = new LiteDatabase("lite.db")) {
        using (var trans = db.BeginTrans()) {
          try {
            db.GetCollection("SomeData").Insert(new BsonDocument() {
              ["Data"] = (objectId + count).ToString()
            });
            db.FileStorage.Delete(objectId);
            using (var stream = db.FileStorage.OpenWrite(objectId, objectId)) {
              using (var writer = new StreamWriter(stream)) {
                for (int i = 0; i < lines.Length; i++) {
                  if (i == lines.Length / 2 && count % 5 == 0)
                    trans.Rollback();
                  writer.WriteLine(lines[i]);
                }
              }
            }
          }
          catch (Exception ex) {
            Console.WriteLine(ex.GetType().Name + " " + ex.Message);
            trans.Rollback();
          }

          trans.Commit();
        }
      }
    }
  }
}
