points = LOAD '$points_input' USING PigStorage(',') AS (id:long, x:double, y:double);
qx= LOAD '$qx' AS (qx:double);
qy= LOAD '$qy' AS (qy:double);
k= LOAD '$k';
d= FOREACH points {
	dist=SQRT(((x-(double)'$qx')*(x-(double)'$qx')) + ((y-(double)'$qy')*(y-(double)'$qy')));
	GENERATE dist,x,y;
};

dump d;