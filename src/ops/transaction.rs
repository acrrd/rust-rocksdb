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
    ffi, handle::Handle, transaction::Transaction, transaction_db::TransactionDB,
    TransactionOptions, WriteOptions,
};

use std::ptr;

pub trait TransactionBegin {
    fn transaction(&self) -> Transaction;
}

pub trait TransactionBeginOpt<T> {
    fn transaction_opt(&self, wrt_opts: &WriteOptions, tx_opts: T) -> Transaction;
}

impl<T> TransactionBegin for T
where
    for<'a> T: TransactionBeginOpt<&'a TransactionOptions>,
{
    fn transaction(&self) -> Transaction {
        self.transaction_opt(
            &WriteOptions::default(),
            &TransactionOptions::default(),
        )
    }
}

impl TransactionBeginOpt<&TransactionOptions> for TransactionDB{
    fn transaction_opt(
        &self,
        writeopts: &WriteOptions,
        txopts: &TransactionOptions,
    ) -> Transaction {
        unsafe {
            let inner = ffi::rocksdb_transaction_begin(
                self.handle(),
                writeopts.inner,
                txopts.inner,
                ptr::null_mut(),
            );
            Transaction::new(inner)
        }
    }
}
