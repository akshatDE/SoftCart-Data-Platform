
# The LOAD DATA INFILE command requires a special privilege. 
# Your softcart_user doesn't have it. Grant it via root:

docker exec -i -e MYSQL_PWD="rootpass123" mysql_db mysql --user=root -e "
GRANT FILE ON *.* TO 'softcart_user'@'%';
FLUSH PRIVILEGES;
"
