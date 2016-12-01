from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "1234"))
session = driver.session()

# Llenado
session.run("CREATE (r:region {n_name:'Spain',r_name: 'Catalunya',min_supplycost:40})")
session.run("CREATE (o:order {o_key:1, c_mktsegment: 'hombre', o_orderdate: '2016/02/07', o_shippriority: 1})")
session.run("MATCH(r:region {r_name:'Catalunya'}) MATCH(o:order {o_key:1}) CREATE(r)-[w:regiontoorder]->(o)")
session.run(
    "CREATE (l:LineItem {returnflag:'t',shipdate: '2016/02/06', linestatus: 't', quantity: 2, extendedprice: 50,"
    " discount: 10, "
    "tax: 10})")
session.run("MATCH(l:LineItem {returnflag:'t'}) MATCH(o:order {o_key:1}) CREATE(o)-[w:ordertoline]->(l)")
session.run("CREATE (s:supplier {s_name:'Roberto',s_acctbal: 5, s_address: 'aki', s_phone: '555555', s_comment:"
            " 'hola'})")
session.run("MATCH(l:LineItem {returnflag:'t'}) MATCH(s:supplier {s_name:'Roberto'}) CREATE(l)-[w:linetosupplier]->(s)")
session.run("CREATE (p:part {p_partyKey: 2, p_mfgr: 'nose', p_size: 10, p_type: 'cosa'})")
session.run(
    "MATCH(p:part {p_partyKey: 2}) MATCH(s:supplier {s_name:'Roberto'}) CREATE(s)-[w:suppliertopart {supplycost:40}]->(p)")
session.run("MATCH(r:region {n_name:'Spain'}) MATCH(s:supplier {s_name:'Roberto'}) CREATE(s)<-[w:suppliertoregion]-(r)")


def queryOne(date):
    result = session.run("MATCH (l:LineItem {}) "
                         "WHERE l.shipdate <= %s "
                         "RETURN l.returnflag, l.linestatus, SUM(l.quantity) AS sum_qty, SUM(l.extendedprice) AS sum_base_price, SUM(l.extendedprice*(1-l.discount)) AS sum_disc_price,"
                         "SUM(l.extendedprice*(1-l.discount)*(1+l.tax)) AS sum_charge, AVG(l.quantity) AS avg_qty,"
                         " AVG(l.extendedprice) AS avg_price, AVG(l.discount) AS avg_disc, COUNT(*) AS count_order "
                         " ORDER BY l.returnflag, l.linestatus" % (date))
    return result


def queryTwo(region_name, size, type_):
    query = "MATCH (r:region {r_name:'%s'}) --> (s:supplier{}) -[sp:suppliertopart]->(p:part {p_size: %s }) " \
            "WHERE sp.supplycost = r.min_supplycost AND p.p_type=~ '%s$' " \
            "RETURN s.s_acctbal, s.s_name, r.n_name,p.p_partkey,p.p_mfgr, s.s_address, s.s_phone, s.s_comment " \
            "ORDER BY s.s_acctbal DESC, r.n_name, s.s_name,p.partkey" % (region_name, str(size), type_)
    return session.run(
        query
    )


def queryThree(mkt_segment, ini_d, end_d):
    query = "MATCH (o:order {c_mktsegment:'%s'}) --> (l:LineItem {}) " \
            "WHERE o.o_orderdate < %s AND l.shipdate > %s " \
            "RETURN o.o_key,o.o_orderdate, o.o_shippriority,  SUM(l.extendedprice*(1-l.discount)) AS revenue " \
            "ORDER BY revenue DESC, o.o_orderdate" % (mkt_segment, end_d, ini_d)
    return session.run(query)


def queryFour(region, date, date2):
    result = session.run("MATCH (r:region {r_name: %s }) - - > (o:order) - - > (l:LineItem) - - > "
                         "(s:supplier) - - (r2:region) "
                         "WHERE o.o_orderdate >= %s AND o.o_orderdate < %s AND r.n_name = r2.n_name "
                         "RETURN r.n_name, SUM(l.extendedprice*(1-l.discount)) AS revenue "
                         "ORDER BY revenue DESC" % (region, date, date2))
    return result


def query_print(result):
    for record in result:
        print(record)


print('QUERY 1')
query_print(queryOne("'2016/03/06'"))
print('QUERY 2')
query_print(queryTwo('Catalunya', 10, 'cosa'))
print('QUERY 3')
query_print(queryThree('hombre', "'2015/01/06'", "'2018/03/06'"))
print('QUERY 4')
query_print(queryFour("'Catalunya'", "'2016/01/06'", "'2017/03/06'"))

# Borrado

print('DELETING DB AGAIN')
session.run("MATCH (n) DETACH DELETE n")

session.close()
