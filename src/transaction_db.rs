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
    ffi_util::to_cpath,
    handle::Handle,
    ops::{
        column_family::GetColumnFamilies,
        snapshot::SnapshotInternal,
    },
    ColumnFamily, ColumnFamilyDescriptor, Error, Options, Snapshot, TransactionDBOptions,
    DEFAULT_COLUMN_FAMILY_NAME,
};

// use ambassador::Delegate;
// use delegate::delegate;
use libc::{self, c_char, c_int};
use std::collections::BTreeMap;
use std::ffi::CString;
use std::fmt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;

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
        txopts: &TransactionDBOptions,
        cfs: I,
    ) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = ColumnFamilyDescriptor>,
    {
        let cfs: Vec<_> = cfs.into_iter().collect();

        let cpath = to_cpath(&path)?;

        if let Err(e) = fs::create_dir_all(&path) {
            return Err(Error::new(format!(
                "Failed to create RocksDB directory: `{:?}`.",
                e
            )));
        }

        let db: *mut ffi::rocksdb_transactiondb_t;
        let mut cf_map = BTreeMap::new();

        if cfs.is_empty() {
            db = Self::open_raw(opts, txopts, &cpath)?;
        } else {
            let mut cfs_v = cfs;
            // Always open the default column family.
            if !cfs_v.iter().any(|cf| cf.name == DEFAULT_COLUMN_FAMILY_NAME) {
                cfs_v.push(ColumnFamilyDescriptor {
                    name: String::from(DEFAULT_COLUMN_FAMILY_NAME),
                    options: Options::default(),
                });
            }
            // We need to store our CStrings in an intermediate vector
            // so that their pointers remain valid.
            let c_cfs: Vec<CString> = cfs_v
                .iter()
                .map(|cf| CString::new(cf.name.as_bytes()).unwrap())
                .collect();

            let cfnames: Vec<_> = c_cfs.iter().map(|cf| cf.as_ptr()).collect();

            // These handles will be populated by DB.
            let mut cfhandles: Vec<_> = cfs_v.iter().map(|_| ptr::null_mut()).collect();

            let cfopts: Vec<_> = cfs_v
                .iter()
                .map(|cf| cf.options.inner as *const _)
                .collect();

            db = Self::open_cf_raw(
                opts,
                txopts,
                &cpath,
                &cfs_v,
                &cfnames,
                &cfopts,
                &mut cfhandles,
            )?;
            for handle in &cfhandles {
                if handle.is_null() {
                    return Err(Error::new(
                        "Received null column family handle from DB.".to_owned(),
                    ));
                }
            }

            for (cf_desc, inner) in cfs_v.iter().zip(cfhandles) {
                cf_map.insert(cf_desc.name.clone(), ColumnFamily { inner });
            }
        }

        if db.is_null() {
            return Err(Error::new("Could not initialize database.".to_owned()));
        }

        Ok(Self {
            inner: db,
            cfs: cf_map,
            path: path.as_ref().to_path_buf(),
        })
    }

    fn open_raw(
        opts: &Options,
        txopts: &TransactionDBOptions,
        cpath: &CString,
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
        txopts: &TransactionDBOptions,
        cpath: &CString,
        cfs_v: &[ColumnFamilyDescriptor],
        cfnames: &[*const c_char],
        cfopts: &[*const ffi::rocksdb_options_t],
        cfhandles: &mut Vec<*mut ffi::rocksdb_column_family_handle_t>,
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
        Self::open_cf_opt(opts, path, txopts, None::<&str>)
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
        Self::open_cf_opt(opts, path, &txopts, cfs)
    }

    /// Opens a database with the given database options and column family names.
    /// Column families opened using this function will be created with default `Options`.
    pub fn open_cf_opt<P, I, N>(
        opts: &Options,
        path: P,
        txopts: &TransactionDBOptions,
        cfs: I,
    ) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let cfs = cfs
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name.as_ref(), Options::default()));

        Self::open_cf_descriptors_internal(opts, path, txopts, cfs)
    }

    /// Opens a database with the given database options and column family descriptors.
    pub fn open_cf_descriptors<P, I>(opts: &Options, path: P, cfs: I) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = ColumnFamilyDescriptor>,
    {
        let txopts = TransactionDBOptions::default();
        Self::open_cf_descriptors_internal(opts, path, &txopts, cfs)
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
