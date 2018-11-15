points = LOAD '$points_input' USING PigStorage(',') AS (id:long, x:double, y:double);
dump points;