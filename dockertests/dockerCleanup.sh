pgname=apidSync_test_pg
ssname=apidSync_test_ss
csname=apidSync_test_cs
docker kill ${pgname} ${csname} ${ssname}
docker rm -f ${pgname} ${csname} ${ssname}