import sys
sys.path.append("src")
from storage import query

sql = sys.argv[1] if len(sys.argv) > 1 else "SELECT count(*) AS n FROM t"
layer = sys.argv[2] if len(sys.argv) > 2 else "bronze"
table = sys.argv[3] if len(sys.argv) > 3 else "acled_events"

print(query(sql, layer, table))