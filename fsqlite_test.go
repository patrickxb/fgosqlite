package fsqlite

import (
	"testing"
        "fmt"
)

func TestOpen(t *testing.T) {
	db, err := Open("/tmp/test.db")
	if err != nil {
		t.Errorf("couldn't open database file: %s", err)
	}
	if db == nil {
		t.Error("opened database is nil")
	}
	db.Close()
}

func TestCreateTable(t *testing.T) {
	db, err := Open("/tmp/test.db")
	db.Exec("DROP TABLE test")
	err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, float_num REAL, int_num INTEGER, a_string TEXT)")
	if err != nil {
		t.Errorf("error creating table: %s", err)
	}
}

type OutRow struct {
	Key      int64
	FloatNum float64
	IntNum   int64
	AString  string
}

func TestInsert(t *testing.T) {
	db, _ := Open("/tmp/test.db")
	db.Exec("DROP TABLE test")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, float_num REAL, int_num INTEGER, a_string TEXT)")
	for i := 0; i < 1000; i++ {
		ierr := db.Exec("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)", float64(i)*float64(3.14), i, "hello")
		if ierr != nil {
			t.Errorf("insert error: %s", ierr)
		}
	}

	cs, _ := db.Prepare("SELECT COUNT(*) FROM test")
	cs.Exec()
	if !cs.Next() {
		t.Error("no result for count")
	}
	var i int
	err := cs.Scan(&i)
	if err != nil {
		t.Errorf("error scanning count: %s", err)
	}
	if i != 1000 {
		t.Errorf("count should be 1000, but it is %d", i)
	}
}

func TestInsertWithStatement(t *testing.T) {
	db, _ := Open("/tmp/test_is.db")
	db.Exec("DROP TABLE test")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, float_num REAL, int_num INTEGER, a_string TEXT)")
	s, serr := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)")
	if serr != nil {
		t.Errorf("prepare error: %s", serr)
	}
	if s == nil {
		t.Error("statement is nil")
	}

	for i := 0; i < 1000; i++ {
		ierr := s.Exec(float64(i)*float64(3.14), i, "hello")
		if ierr != nil {
			t.Errorf("insert error: %s", ierr)
		}
		s.Next()
	}
	s.Finalize()

	cs, _ := db.Prepare("SELECT COUNT(*) FROM test")
	cs.Exec()
	if !cs.Next() {
		t.Error("no result for count")
	}
	var i int
	err := cs.Scan(&i)
	if err != nil {
		t.Errorf("error scanning count: %s", err)
	}
	if i != 1000 {
		t.Errorf("count should be 1000, but it is %d", i)
	}
}

func TestInsertWithStatement2(t *testing.T) {
	db, _ := Open("/tmp/test_is2.db")
	db.Exec("DROP TABLE test")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, float_num REAL, int_num INTEGER, a_string TEXT)")
	s, serr := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)")
	if serr != nil {
		t.Errorf("prepare error: %s", serr)
	}
	if s == nil {
		t.Error("statement is nil")
	}

	for i := 0; i < 1000; i++ {
		ierr := s.Exec2(float64(i)*float64(3.14), i, "hello")
		if ierr != nil {
			t.Errorf("insert error: %s", ierr)
		}
		s.Next()
	}
	s.Finalize()

	cs, _ := db.Prepare("SELECT COUNT(*) FROM test")
	cs.Exec()
	if !cs.Next() {
		t.Error("no result for count")
	}
	var i int
	err := cs.Scan(&i)
	if err != nil {
		t.Errorf("error scanning count: %s", err)
	}
	if i != 1000 {
		t.Errorf("count should be 1000, but it is %d", i)
	}

        rs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test ORDER BY int_num LIMIT 10")
	var fnum float64
	var inum int64
	var sstr string
	for rs.Next() {
			rs.Scan(&fnum, &inum, &sstr)
                        fmt.Printf("fnum = %f, inum = %d, sstre = %s\n", fnum, inum, sstr)
		        }
}

func BenchmarkScan(b *testing.B) {
	b.StopTimer()
	db, _ := Open("/tmp/test_bs.db")
	db.Exec("DROP TABLE test")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, float_num REAL, int_num INTEGER, a_string TEXT)")
	s, _ := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)")

	for i := 0; i < 1000; i++ {
		s.Exec(float64(i)*float64(3.14), i, "hello")
		s.Next()
	}
	s.Finalize()

	b.StartTimer()
	for i := 0; i < b.N; i++ {

	cs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test")
	cs.Exec()

	var fnum float64
	var inum int64
	var sstr string

		for cs.Next() {
			cs.Scan(&fnum, &inum, &sstr)
		}
	}
}

func BenchmarkScan2(b *testing.B) {
	b.StopTimer()
	db, _ := Open("/tmp/test_bs2.db")
	db.Exec("DROP TABLE test")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, float_num REAL, int_num INTEGER, a_string TEXT)")
	s, _ := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)")

	for i := 0; i < 1000; i++ {
		s.Exec(float64(i)*float64(3.14), i, "hello")
		s.Next()
	}
	s.Finalize()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
	cs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test")
	cs.Exec()

	var fnum float64
	var inum int64
	var sstr string

		for cs.Next() {
			cs.Scan2(&fnum, &inum, &sstr)
		}
	}
}

func BenchmarkInsert(b *testing.B) {
	db, _ := Open("/tmp/test_bi.db")
	db.Exec("DROP TABLE test")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, float_num REAL, int_num INTEGER, a_string TEXT)")
	s, _ := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)")

//	for x := 0; x < b.N; x++ {
                for i := 0; i < b.N; i++ {
                        s.Exec(float64(i)*float64(3.14), i, "hello")
                                s.Next()
                }
//        }
	s.Finalize()
}

func BenchmarkInsert2(b *testing.B) {
	db, _ := Open("/tmp/test_bi2.db")
	db.Exec("DROP TABLE test")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, float_num REAL, int_num INTEGER, a_string TEXT)")
	s, _ := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)")

//	for x := 0; x < b.N; x++ {
                for i := 0; i < b.N; i++ {
                        s.Exec2(float64(i)*float64(3.14), i, "hello")
                                s.Next()
                }
//        }
	s.Finalize()
}
