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

Query product images with product details using DB2:
```
            $images = DB2::table('catalog_product_entity_media_gallery as main_table')
                ->join(
                    'catalog_product_entity_media_gallery_value as mgv',
                    'mgv.value_id',
                    '=',
                    'main_table.value_id'
                )
                ->join(
                    'catalog_product_entity as e',
                    'e.entity_id',
                    '=',
                    'mgv.entity_id'
                )
                ->select([
                    'main_table.value_id',
                    'main_table.attribute_id',
                    'main_table.value',
                    'main_table.media_type',
                    'main_table.disabled',
                    'e.sku'
                ])
                ->where('main_table.media_type', '=', 'image')
                ->where('main_table.disabled', '=', 0)
                ->orderBy('main_table.value_id', 'ASC')
                ->get();
```
