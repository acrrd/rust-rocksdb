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

use crate::{
    ffi, handle::Handle, make_vec_from_val_ptr, ops::SnapshotInternal, ColumnFamily, Error,
    ReadOptions, Snapshot,
};

use libc::{c_uchar, size_t, c_void, c_char};
use std::marker::PhantomData;

/// A transaction.
pub struct Transaction<'a> {
    inner: *mut ffi::rocksdb_transaction_t,
    _db: PhantomData<&'a ()>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(inner: *mut ffi::rocksdb_transaction_t) -> Self {
        Self {
            inner,
            _db: PhantomData,
        }
    }

    /// commits a transaction
    pub fn commit(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_commit(self.inner));
        }
        Ok(())
    }

    /// Transaction rollback
    pub fn rollback(&self) -> Result<(), Error> {
        unsafe { ffi_try!(ffi::rocksdb_transaction_rollback(self.inner,)) }
        Ok(())
    }

    /// Transaction rollback to savepoint
    pub fn rollback_to_savepoint(&self) -> Result<(), Error> {
        unsafe { ffi_try!(ffi::rocksdb_transaction_rollback_to_savepoint(self.inner,)) }
        Ok(())
    }

    /// Set savepoint for transaction
    pub fn set_savepoint(&self) {
        unsafe { ffi::rocksdb_transaction_set_savepoint(self.inner) }
    }

    // Get for update with default options and exlusive set to true
    pub fn get_for_update<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Error> {
        let opt = ReadOptions::default();
        self.get_for_update_opt(key, &opt, true)
    }

    // Get for update
    pub fn get_for_update_opt<K: AsRef<[u8]>>(
        &self,
        key: K,
        readopts: &ReadOptions,
        exclusive: bool,
    ) -> Result<Option<Vec<u8>>, Error> {
        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;
        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transaction_get_for_update(
                self.handle(),
                readopts.inner,
                key_ptr,
                key_len,
                &mut val_len,
                exclusive as c_uchar,
            )) as *mut u8;

            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(make_vec_from_val_ptr(val, val_len)))
            }
        }
    }

    // Get for update with default options and exlusive set to true with column family
    pub fn get_for_update_cf<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
    ) -> Result<Option<Vec<u8>>, Error> {
        let opt = ReadOptions::default();
        self.get_for_update_cf_opt(cf, key, &opt, true)
    }

    pub fn get_for_update_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
        readopts: &ReadOptions,
        exclusive: bool,
    ) -> Result<Option<Vec<u8>>, Error> {
        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;
        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transaction_get_for_update_cf(
                self.handle(),
                readopts.inner,
                cf.inner,
                key_ptr,
                key_len,
                &mut val_len,
                exclusive as c_uchar,
            )) as *mut u8;

            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(make_vec_from_val_ptr(val, val_len)))
            }
        }
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transaction_destroy(self.inner);
        }
    }
}

impl<'a> Handle<ffi::rocksdb_transaction_t> for Transaction<'a> {
    fn handle(&self) -> *mut ffi::rocksdb_transaction_t {
        self.inner
    }
}

impl<'a> SnapshotInternal for Transaction<'a> {
    type DB = Self;

    unsafe fn create_snapshot(&self) -> Snapshot<Self> {
        let inner = ffi::rocksdb_transaction_get_snapshot(self.handle());
        Snapshot { db: self, inner }
    }

    unsafe fn release_snapshot(&self, snapshot: &mut Snapshot<Self>) {
        ffi::rocksdb_free(snapshot.inner as *mut c_void);
    }
}
