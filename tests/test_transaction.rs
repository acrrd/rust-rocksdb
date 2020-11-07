// Copyright 2020 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod util;

use pretty_assertions::assert_eq;

use rocksdb::{
    prelude::*, MergeOperands, TransactionDB, TransactionDBOptions, TransactionOptions,
};
use util::DBPath;

#[test]
fn transaction() {
    let path = DBPath::new("_rust_rocksdb_transaction");

    {
        let db = TransactionDB::open_default(&path).unwrap();

        let trans = db.transaction();

        trans.put(b"k1", b"v1").unwrap();
        trans.put(b"k2", b"v2").unwrap();
        trans.put(b"k3", b"v3").unwrap();
        trans.put(b"k4", b"v4").unwrap();

        trans.commit().unwrap();

        let trans2 = db.transaction();

        let mut iter = trans2.raw_iterator();

        iter.seek_to_first();

        assert_eq!(iter.valid(), true);
        assert_eq!(iter.key(), Some(&b"k1"[..]));
        assert_eq!(iter.value(), Some(&b"v1"[..]));

        iter.next();

        assert_eq!(iter.valid(), true);
        assert_eq!(iter.key(), Some(&b"k2"[..]));
        assert_eq!(iter.value(), Some(&b"v2"[..]));

        iter.next(); // k3
        iter.next(); // k4
        iter.next(); // invalid!

        assert_eq!(iter.valid(), false);
        assert_eq!(iter.key(), None);
        assert_eq!(iter.value(), None);

        let trans3 = db.transaction();

        trans2.put(b"k2", b"v5").unwrap();
        trans3.put(b"k2", b"v6").unwrap_err();

        trans3.commit().unwrap();

        trans2.commit().unwrap();
    }
}

#[test]
pub fn transaction_rollback_savepoint() {
    let path = DBPath::new("_rust_rocksdb_transaction_rollback_savepoint");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = TransactionDB::open(&opts, &path).unwrap();

        let trans1 = db.transaction();
        let trans2 = db.transaction();

        trans1.put(b"k1", b"v1").unwrap();

        let k1_2 = trans2.get(b"k1").unwrap();
        assert!(k1_2.is_none());

        trans1.commit().unwrap();

        let trans3 = db.transaction();

        let k1_2 = trans2.get(b"k1").unwrap().unwrap();
        assert_eq!(&*k1_2, b"v1");

        trans3.delete(b"k1").unwrap();

        let k1_2 = trans2.get(b"k1").unwrap().unwrap();
        assert_eq!(&*k1_2, b"v1");

        trans3.rollback().unwrap();

        let k1_2 = trans2.get(b"k1").unwrap().unwrap();
        assert_eq!(&*k1_2, b"v1");

        let trans4 = db.transaction();

        trans4.delete(b"k1").unwrap();
        trans4.set_savepoint();
        trans4.put(b"k2", b"v2").unwrap();
        trans4.rollback_to_savepoint().unwrap();
        trans4.commit().unwrap();

        let k1_2 = trans2.get(b"k1").unwrap();
        assert!(k1_2.is_none());

        let k2_2 = trans2.get(b"k2").unwrap();
        assert!(k2_2.is_none());

        trans2.commit().unwrap();
    }
}

#[test]
pub fn transaction_snapshot() {
    let path = DBPath::new("_rust_rocksdb_transaction_snapshot");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = TransactionDB::open(&opts, &path).unwrap();

        let write_options = WriteOptions::default();
        let transaction_options = TransactionOptions::new();
        let trans1 = db.transaction_opt(&write_options, &transaction_options);

        let mut transaction_options_snapshot = TransactionOptions::new();
        transaction_options_snapshot.set_snapshot(true);
        // create transaction with snapshot
        let trans2 = db.transaction_opt(&write_options, &transaction_options_snapshot);

        trans1.put(b"k1", b"v1").unwrap();

        let k1_2 = trans2.get(b"k1").unwrap();
        assert!(k1_2.is_none());

        trans1.commit().unwrap();
        drop(trans1);

        trans2.commit().unwrap();
        drop(trans2);

        let trans3 = db.transaction_opt(&write_options, &transaction_options_snapshot);

        let trans4 = db.transaction_opt(&write_options, &transaction_options);
        trans4.delete(b"k1").unwrap();
        trans4.commit().unwrap();
        drop(trans4);

        assert!(trans3.get(b"k1").unwrap().is_none());

        let k1_3 = trans3.snapshot().get(b"k1").unwrap().unwrap();
        assert_eq!(&*k1_3, b"v1");

        trans3.commit().unwrap();
        drop(trans3);

        let trans5 = db.transaction_opt(&write_options, &transaction_options_snapshot);

        let k1_5 = trans5.snapshot().get(b"k1").unwrap();
        assert!(k1_5.is_none());

        trans5.commit().unwrap();
    }
}

#[test]
pub fn get_for_update() {
    let path = DBPath::new("_rust_rocksdb_get_for_update");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let txopts = TransactionDBOptions::default();

        let db = TransactionDB::open_opt(&opts, &path, &txopts).unwrap();

        db.put("k1", "v1").expect("failed to put k1 v1");
        let v1 = db
            .get("k1")
            .expect("failed to get k1")
            .expect("k1 is not exists");
        assert_eq!(&*v1, b"v1");

        let tran1 = db.transaction();
        let v1 = tran1
            .get_for_update("k1")
            .expect("failed to get for update k1")
            .expect("k1 is not exists");
        assert_eq!(&*v1, b"v1");

        assert!(db.put("k1", "v2").is_err());

        let v1 = tran1
            .get_for_update("k1")
            .expect("failed to get for update k1")
            .expect("k1 is not exists");
        assert_eq!(&*v1, b"v1");

        tran1.put("k2", "v2").expect("failed to put k1 v1");
        tran1.commit().unwrap();
    }
}

#[test]
pub fn get_for_update_cf() {
    let path = DBPath::new("_rust_rocksdb_get_for_update_cf");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let txopts = TransactionDBOptions::default();

        let mut db = TransactionDB::open_opt(&opts, &path, &txopts).unwrap();

        db.create_cf("cf1", &opts)
            .expect("failed to create new column family cf1");
        let cf1 = db.cf_handle("cf1").expect("column family not exists.");

        db.put_cf(cf1, "k1", "v1").expect("failed to put k1 v1");
        let v1 = db
            .get_cf(cf1, "k1")
            .expect("failed to get k1")
            .expect("k1 is not exists");
        assert_eq!(&*v1, b"v1");

        let tran1 = db.transaction();
        let v1 = tran1
            .get_for_update_cf(cf1, "k1")
            .expect("failed to get for update k1")
            .expect("k1 is not exists");
        assert_eq!(&*v1, b"v1");

        assert!(db.put_cf(cf1, "k1", "v2").is_err());

        let v1 = tran1
            .get_for_update_cf(cf1, "k1")
            .expect("failed to get for update k1")
            .expect("k1 is not exists");
        assert_eq!(&*v1, b"v1");

        tran1.put_cf(cf1, "k2", "v2").expect("failed to put k1 v1");
        tran1.commit().unwrap();
    }
}

#[test]
pub fn test_transaction_merge() {
    fn concat_merge(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Vec<u8> = Vec::with_capacity(operands.size_hint().0);
        existing_val.map(|v| {
            for e in v {
                result.push(*e)
            }
        });
        for op in operands {
            for e in op {
                result.push(*e)
            }
        }
        Some(result)
    }

    let path = DBPath::new("_rust_rocksdb_transaction_merge");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator_associative("test operator", concat_merge);
        let db = TransactionDB::open(&opts, &path).unwrap();
        let trans1 = db.transaction();

        trans1.put(b"k1", b"a").unwrap();
        trans1.merge(b"k1", b"b").unwrap();
        trans1.merge(b"k1", b"c").unwrap();
        trans1.merge(b"k1", b"d").unwrap();
        trans1.merge(b"k1", b"efg").unwrap();
        trans1.get(b"k1").err().unwrap();
        trans1.commit().unwrap();

        let trans2 = db.transaction();
        let k1 = trans2.get(b"k1").unwrap().unwrap();
        assert_eq!(&*k1, b"abcdefg");

        trans2.commit().unwrap();
    }
}
