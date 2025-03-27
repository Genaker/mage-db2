# install 

```
composer require mage/db2
```
If you have any conflicts, you can use:

```
composer require mage/db2 --with-all-dependencies --ignore-platform-reqs --prefer-source --no-scripts

```
use  Mage\DB2\DB2;

$isReturningCustomer = DB2::table('sales_order')
            ->where('customer_id', $customerId)
            ->where('entity_id', '<', $entityId)
            ->exists();
```

```
DB2::table('sales_order')->where('customer_id', 123)->get();
```
