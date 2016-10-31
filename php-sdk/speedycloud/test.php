<?php
require_once "/home/wsx/SDK/php-sdk/speedycloud/object_storage.php";
$aa = new Object_Storage("access_key","secret_key");
echo $aa->object_list("baka");
?>