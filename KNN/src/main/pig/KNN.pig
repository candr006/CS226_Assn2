points = LOAD '$points_input' USING PigStorage(',') AS (id:long, x:double, y:double);
qx= LOAD '$qx' AS (qx:double);
qy= LOAD '$qy' AS (qy:double);
d= FOREACH points {
	dist=SQRT(((x-(double)'$qx')*(x-(double)'$qx')) + ((y-(double)'$qy')*(y-(double)'$qy')));
	GENERATE dist,x,y;
};

d_group= group d by ($0,$1,$2);
d_reduced= FOREACH d_group {
	 unique = LIMIT d 1;
     GENERATE FLATTEN(unique);
};
d_sorted= order d_reduced by ($0);
d_limited = LIMIT d_sorted (long)$k;

dump d_limited;