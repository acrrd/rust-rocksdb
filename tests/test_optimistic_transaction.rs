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
    prelude::*, MergeOperands, OptimisticTransactionDB, OptimisticTransactionOptions, WriteOptions,
};
use util::DBPath;

#[test]
fn test_optimistic_transactiondb() {
    let path = DBPath::new("_rust_rocksdb_optimistic_transaction_db");
    {
        let db = OptimisticTransactionDB::open_default(&path).unwrap();
        db.put(b"k1", b"v1").unwrap();
        assert_eq!(db.get(b"k1").unwrap().unwrap(), b"v1");
    }
}

#[test]
pub fn test_optimistic_transaction() {
    let path = DBPath::new("_rust_rocksdb_optimistic_transaction");
    {
        let db = OptimisticTransactionDB::open_default(&path).unwrap();

        let trans = db.transaction();

        trans.put(b"k1", b"v1").unwrap();
        trans.put(b"k2", b"v2").unwrap();
        trans.put(b"k3", b"v3").unwrap();
        trans.put(b"k4", b"v4").unwrap();

        let trans_result = trans.commit();

        assert_eq!(trans_result.is_ok(), true);

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
        trans3.put(b"k2", b"v6").unwrap();

        trans3.commit().unwrap();

        trans2.commit().unwrap_err();
    }
}

#[test]
pub fn test_optimistic_transaction_rollback_savepoint() {
    let path = DBPath::new("_rust_rocksdb_optimistic_transaction_rollback_savepoint");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = OptimisticTransactionDB::open(&opts, &path).unwrap();
        let write_options = WriteOptions::default();
        let optimistic_transaction_options = OptimisticTransactionOptions::new();

        let trans1 = db.transaction_opt(&write_options, &optimistic_transaction_options);
        let trans2 = db.transaction_opt(&write_options, &optimistic_transaction_options);

        trans1.put(b"k1", b"v1").unwrap();

        let k1_2 = trans2.get(b"k1").unwrap();
        assert!(k1_2.is_none());

        trans1.commit().unwrap();

        let k1_2 = trans2.get(b"k1").unwrap().unwrap();
        assert_eq!(&*k1_2, b"v1");

        trans1.delete(b"k1").unwrap();

        let k1_2 = trans2.get(b"k1").unwrap().unwrap();
        assert_eq!(&*k1_2, b"v1");

        trans1.rollback().unwrap();

        let k1_2 = trans2.get(b"k1").unwrap().unwrap();
        assert_eq!(&*k1_2, b"v1");

        trans1.delete(b"k1").unwrap();
        trans1.set_savepoint();
        trans1.put(b"k2", b"v2").unwrap();
        trans1.rollback_to_savepoint().unwrap();
        trans1.commit().unwrap();

        let k1_2 = trans2.get(b"k1").unwrap();
        assert!(k1_2.is_none());

        let k2_2 = trans2.get(b"k2").unwrap();
        assert!(k2_2.is_none());

        trans2.commit().unwrap();
    }
}

#[test]
pub fn test_optimistic_transaction_cf() {
    let path = DBPath::new("_rust_rocksdb_optimistic_transaction_cf");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let mut db = OptimisticTransactionDB::open_cf(&opts, &path, &["cf1"]).unwrap();
        {
            let cf_handle = db.cf_handle("cf1").unwrap();
            let write_options = WriteOptions::default();
            let optimistic_transaction_options = OptimisticTransactionOptions::new();

            let trans = db.transaction_opt(&write_options, &optimistic_transaction_options);

            trans.put_cf(cf_handle, b"k1", b"v1").unwrap();
            trans.commit().unwrap();

            let k1 = trans.get_cf(cf_handle, b"k1").unwrap().unwrap();
            assert_eq!(&*k1, b"v1");

            trans.delete_cf(cf_handle, b"k1").unwrap();
            trans.commit().unwrap();
        }

        db.drop_cf("cf1").unwrap();
    }
}

#[test]
pub fn test_optimistic_transaction_snapshot() {
    let path = DBPath::new("_rust_rocksdb_optimistic_transaction_snapshot");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = OptimisticTransactionDB::open(&opts, &path).unwrap();

        let write_options = WriteOptions::default();
        let optimistic_transaction_options = OptimisticTransactionOptions::new();
        let trans1 = db.transaction_opt(&write_options, &optimistic_transaction_options);

        let mut optimistic_transaction_options_snapshot = OptimisticTransactionOptions::new();
        optimistic_transaction_options_snapshot.set_snapshot(true);
        // create transaction with snapshot
        let trans2 = db.transaction_opt(&write_options, &optimistic_transaction_options_snapshot);

        trans1.put(b"k1", b"v1").unwrap();

        let k1_2 = trans2.get(b"k1").unwrap();
        assert!(k1_2.is_none());

        trans1.commit().unwrap();

        trans2.commit().unwrap();
        drop(trans2);

        let trans3 = db.transaction_opt(&write_options, &optimistic_transaction_options_snapshot);

        trans1.delete(b"k1").unwrap();
        trans1.commit().unwrap();

        assert!(trans3.get(b"k1").unwrap().is_none());

        let k1_3 = trans3.snapshot().get(b"k1").unwrap().unwrap();
        assert_eq!(&*k1_3, b"v1");

        trans3.commit().unwrap();
        drop(trans3);

        let trans4 = db.transaction_opt(&write_options, &optimistic_transaction_options_snapshot);

        let k1_4 = trans4.snapshot().get(b"k1").unwrap();
        assert!(k1_4.is_none());

        trans4.commit().unwrap();
    }
}

#[test]
pub fn test_optimistic_transaction_merge() {
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

    let path = DBPath::new("_rust_rocksdb_optimistic_transaction_merge");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator_associative("test operator", concat_merge);
        let db = OptimisticTransactionDB::open(&opts, &path).unwrap();
        let trans = db.transaction();

        trans.put(b"k1", b"a").unwrap();
        trans.merge(b"k1", b"b").unwrap();
        trans.merge(b"k1", b"c").unwrap();
        trans.merge(b"k1", b"d").unwrap();
        trans.merge(b"k1", b"efg").unwrap();
        trans.get(b"k1").err().unwrap();
        trans.commit().unwrap();

        let k1 = trans.get(b"k1").unwrap().unwrap();
        assert_eq!(&*k1, b"abcdefg");

        trans.commit().unwrap();
    }
}
