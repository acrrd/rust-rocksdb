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
    ffi, ffi_util::to_cpath, ColumnFamily, ColumnFamilyDescriptor, Error, Options,
    DEFAULT_COLUMN_FAMILY_NAME,
};

use libc::{self, c_char};
use std::collections::BTreeMap;
use std::ffi::CString;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;

/// Internal implementation for opening RocksDB.
pub fn open_cf_descriptors_internal<P, I, D, Ptr, OpenRaw, OpenRawCF>(
    opts: &Options,
    path: P,
    cfs: I,
    descriptor: &D,
    open_raw: OpenRaw,
    open_cf_raw: OpenRawCF,
) -> Result<(*mut Ptr, BTreeMap<String, ColumnFamily>, PathBuf), Error>
where
    P: AsRef<Path>,
    I: IntoIterator<Item = ColumnFamilyDescriptor>,
    OpenRaw: Fn(&Options, &CString, &D) -> Result<*mut Ptr, Error>,
    OpenRawCF: Fn(
    &Options,
    &CString,
    &[ColumnFamilyDescriptor],
    &[*const c_char],
    &[*const ffi::rocksdb_options_t],
    &mut Vec<*mut ffi::rocksdb_column_family_handle_t>,
    &D,
) -> Result<*mut Ptr, Error>,
{
    let cfs: Vec<_> = cfs.into_iter().collect();

    let cpath = to_cpath(&path)?;

    if let Err(e) = fs::create_dir_all(&path) {
        return Err(Error::new(format!(
            "Failed to create RocksDB directory: `{:?}`.",
            e
        )));
    }

    let db: *mut Ptr;
    let mut cf_map = BTreeMap::new();

    if cfs.is_empty() {
        db = open_raw(opts, &cpath, descriptor)?;
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

        db = open_cf_raw(
            opts,
            &cpath,
            &cfs_v,
            &cfnames,
            &cfopts,
            &mut cfhandles,
            descriptor,
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

    Ok((db, cf_map, path.as_ref().to_path_buf()))
}
