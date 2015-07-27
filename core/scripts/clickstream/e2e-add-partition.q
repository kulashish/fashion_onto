use ${dbname};
ALTER TABLE ${tableName} DROP IF EXISTS PARTITION(year1=${year1},month1=${month1},date1=${date1});
ALTER TABLE ${tableName} ADD PARTITION(year1=${year1},month1=${month1},date1=${date1}) LOCATION "${loc}";
