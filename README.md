# install 

```
composer require mage/db2
```
# Usage 

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
