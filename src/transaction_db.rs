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
//

use crate::{
    ffi,
    handle::Handle,
    open_util::{open_cf_descriptors_internal, convert_cfs_to_descriptors},
    ops::{column_family::GetColumnFamilies, snapshot::SnapshotInternal},
    ColumnFamily, ColumnFamilyDescriptor, Error, Options, Snapshot, TransactionDBOptions,
};

// use ambassador::Delegate;
// use delegate::delegate;
use libc::{self, c_char, c_int};
use std::collections::BTreeMap;
use std::ffi::CString;
use std::fmt;
use std::path::Path;
use std::path::PathBuf;

/// A RocksDB database with transaction
///
/// See crate level documentation for a simple usage example.
pub struct TransactionDB {
    pub(crate) inner: *mut ffi::rocksdb_transactiondb_t,
    cfs: BTreeMap<String, ColumnFamily>,
    path: PathBuf,
}

// Safety note: auto-implementing Send on most db-related types is prevented by the inner FFI
// pointer. In most cases, however, this pointer is Send-safe because it is never aliased and
// rocksdb internally does not rely on thread-local information for its user-exposed types.
unsafe impl Send for TransactionDB {}

// Sync is similarly safe for many types because they do not expose interior mutability, and their
// use within the rocksdb library is generally behind a const reference
unsafe impl Sync for TransactionDB {}

impl Handle<ffi::rocksdb_transactiondb_t> for TransactionDB {
    fn handle(&self) -> *mut ffi::rocksdb_transactiondb_t {
        self.inner
    }
}

impl TransactionDB {
    /// Internal implementation for opening RocksDB.
    fn open_cf_descriptors_internal<P, I>(
        opts: &Options,
        path: P,
        cfs: I,
        txopts: &TransactionDBOptions,
    ) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = ColumnFamilyDescriptor>,
    {
        let (inner, cfs, path) = open_cf_descriptors_internal(
            opts,
            path,
            cfs,
            txopts,
            Self::open_raw,
            Self::open_cf_raw,
        )?;

        Ok(Self { inner, cfs, path })
    }

    fn open_raw(
        opts: &Options,
        cpath: &CString,
        txopts: &TransactionDBOptions,
    ) -> Result<*mut ffi::rocksdb_transactiondb_t, Error> {
        let db = unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_open(
                opts.inner,
                txopts.inner,
                cpath.as_ptr() as *const _,
            ))
        };
        Ok(db)
    }

    fn open_cf_raw(
        opts: &Options,
        cpath: &CString,
        cfs_v: &[ColumnFamilyDescriptor],
        cfnames: &[*const c_char],
        cfopts: &[*const ffi::rocksdb_options_t],
        cfhandles: &mut Vec<*mut ffi::rocksdb_column_family_handle_t>,
        txopts: &TransactionDBOptions,
    ) -> Result<*mut ffi::rocksdb_transactiondb_t, Error> {
        let db = unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_open_column_families(
                opts.inner,
                txopts.inner,
                cpath.as_ptr(),
                cfs_v.len() as c_int,
                cfnames.as_ptr(),
                cfopts.as_ptr(),
                cfhandles.as_mut_ptr(),
            ))
        };
        Ok(db)
    }

    /// Opens a database with default options.
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        Self::open(&opts, path)
    }

    /// Opens the database with the specified options.
    pub fn open<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Self, Error> {
        Self::open_cf(opts, path, None::<&str>)
    }

    /// Opens the database with the specified options.
    pub fn open_opt<P: AsRef<Path>>(
        opts: &Options,
        path: P,
        txopts: &TransactionDBOptions,
    ) -> Result<Self, Error> {
        Self::open_cf_opt(opts, path, None::<&str>, txopts)
    }

    /// Opens a database with the given database options and column family names.
    /// Column families opened using this function will be created with default `Options`.
    pub fn open_cf<P, I, N>(opts: &Options, path: P, cfs: I) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let txopts = TransactionDBOptions::default();
        Self::open_cf_opt(opts, path, cfs, &txopts)
    }

    /// Opens a database with the given database options and column family names.
    /// Column families opened using this function will be created with default `Options`.
    pub fn open_cf_opt<P, I, N>(
        opts: &Options,
        path: P,
        cfs: I,
        txopts: &TransactionDBOptions,
    ) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let cfs = convert_cfs_to_descriptors(cfs);

        Self::open_cf_descriptors_internal(opts, path, cfs, txopts)
    }

    /// Opens a database with the given database options and column family descriptors.
    pub fn open_cf_descriptors<P, I>(opts: &Options, path: P, cfs: I) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = ColumnFamilyDescriptor>,
    {
        let txopts = TransactionDBOptions::default();
        Self::open_cf_descriptors_internal(opts, path, cfs, &txopts)
    }

    pub fn path(&self) -> &Path {
        &self.path.as_path()
    }
}

impl GetColumnFamilies for TransactionDB {
    fn get_cfs(&self) -> &BTreeMap<String, ColumnFamily> {
        &self.cfs
    }

    fn get_mut_cfs(&mut self) -> &mut BTreeMap<String, ColumnFamily> {
        &mut self.cfs
    }
}

impl Drop for TransactionDB {
    fn drop(&mut self) {
        unsafe {
            for cf in self.cfs.values() {
                ffi::rocksdb_column_family_handle_destroy(cf.inner);
            }
            ffi::rocksdb_transactiondb_close(self.inner);
        }
    }
}

impl fmt::Debug for TransactionDB {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RocksTransactionDB {{ path: {:?} }}", self.path())
    }
}

impl SnapshotInternal for TransactionDB {
    type DB = Self;

    unsafe fn create_snapshot(&self) -> Snapshot<Self> {
        let inner = ffi::rocksdb_transactiondb_create_snapshot(self.handle());
        Snapshot { db: self, inner }
    }

    unsafe fn release_snapshot(&self, snapshot: &mut Snapshot<Self>) {
        ffi::rocksdb_transactiondb_release_snapshot(self.handle(), snapshot.inner);
    }
}
