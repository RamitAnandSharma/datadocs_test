ALTER TABLE descriptor ADD COLUMN format_name VARCHAR(32);
UPDATE descriptor d SET format_name =
  case d.format
  when 0 then 'JSON'
  when 1 then 'JSON'
  when 2 then 'CSV'
  when 3 then 'Excel'
  when 4 then 'Excel'
  when 5 then 'Excel'
  when 6 then 'Excel'
  when 7 then 'JSON'
  when 8 then 'XML'
  when 9 then 'MySQL'
  when 10 then 'MySQL'
  when 11 then 'MySQL'
  when 12 then 'PostgreSQL'
  when 13 then 'PostgreSQL'
  when 14 then 'PostgreSQL'
  when 15 then 'Oracle'
  when 16 then 'Oracle'
  when 17 then 'Oracle'
  when 18 then 'SQL Server'
  when 19 then 'SQL Server'
  when 20 then 'SQL Server'
  when 24 then 'Avro'
  end;