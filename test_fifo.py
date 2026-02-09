import sqlite3
from datetime import date

DB = r'f:/warehouse_app/warehouse.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

# ensure tables
c.execute("CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY AUTOINCREMENT, article TEXT UNIQUE NOT NULL, name TEXT NOT NULL, unit TEXT)")
c.execute("CREATE TABLE IF NOT EXISTS invoices (id INTEGER PRIMARY KEY AUTOINCREMENT, invoice_number TEXT NOT NULL, invoice_date TEXT NOT NULL, supplier TEXT)")
c.execute("CREATE TABLE IF NOT EXISTS invoice_items (id INTEGER PRIMARY KEY AUTOINCREMENT, invoice_id INTEGER NOT NULL, product_id INTEGER NOT NULL, qty REAL NOT NULL, price REAL NOT NULL, vat_percent REAL NOT NULL, total REAL NOT NULL, total_with_vat REAL NOT NULL)")
c.execute("CREATE TABLE IF NOT EXISTS sales (id INTEGER PRIMARY KEY AUTOINCREMENT, sale_number TEXT NOT NULL, sale_date TEXT NOT NULL, comment TEXT)")
c.execute("CREATE TABLE IF NOT EXISTS sale_items (id INTEGER PRIMARY KEY AUTOINCREMENT, sale_id INTEGER NOT NULL, product_id INTEGER NOT NULL, qty REAL NOT NULL, price REAL NOT NULL, total REAL NOT NULL)")
conn.commit()

# create sale_fifo table if not exists
c.execute('''CREATE TABLE IF NOT EXISTS sale_fifo (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_item_id INTEGER NOT NULL,
    batch_id INTEGER NOT NULL,
    qty REAL NOT NULL,
    cost_price REAL NOT NULL
)
''')
conn.commit()

# Insert product
c.execute("INSERT OR IGNORE INTO products (article, name, unit) VALUES (?, ?, ?)", ('P001', 'Test Product', 'шт'))
conn.commit()
prod_id = c.execute("SELECT id FROM products WHERE article = ?", ('P001',)).fetchone()[0]

# Insert invoice (batch)
c.execute("INSERT INTO invoices (invoice_number, invoice_date, supplier) VALUES (?, ?, ?)", ('INV-1', '2026-01-01', 'Supplier'))
inv_id = c.lastrowid
c.execute("INSERT INTO invoice_items (invoice_id, product_id, qty, price, vat_percent, total, total_with_vat) VALUES (?,?,?,?,?,?,?)",
          (inv_id, prod_id, 10.0, 5.0, 0.0, 50.0, 50.0))
conn.commit()

# Insert sale and sale_item
c.execute("INSERT INTO sales (sale_number, sale_date, comment) VALUES (?,?,?)", ('S-1', '2026-02-01', ''))
sale_id = c.lastrowid
c.execute("INSERT INTO sale_items (sale_id, product_id, qty, price, total) VALUES (?,?,?,?,?)", (sale_id, prod_id, 3.0, 10.0, 30.0))
conn.commit()

# define recalc function locally
def recalc_fifo_for_sale(conn, sale_id):
    conn.execute("""
        DELETE FROM sale_fifo
        WHERE sale_item_id IN (
            SELECT id FROM sale_items WHERE sale_id = ?
        )
    """, (sale_id,))
    conn.commit()

    items = conn.execute(
        "SELECT id, product_id, qty FROM sale_items WHERE sale_id = ?",
        (sale_id,)
    ).fetchall()

    sale_date = conn.execute(
        "SELECT sale_date FROM sales WHERE id = ?",
        (sale_id,)
    ).fetchone()[0]

    for si_id, product_id, qty_need in items:
        batches = conn.execute("""
            SELECT ii.id, ii.qty, ii.price
            FROM invoice_items ii
            JOIN invoices i ON i.id = ii.invoice_id
            WHERE ii.product_id = ? AND i.invoice_date <= ?
            ORDER BY i.invoice_date, ii.id
        """, (product_id, sale_date)).fetchall()

        remaining = qty_need
        for batch_id, batch_qty, cost in batches:
            used = conn.execute(
                "SELECT COALESCE(SUM(qty),0) FROM sale_fifo WHERE batch_id = ?",
                (batch_id,)
            ).fetchone()[0]
            available = batch_qty - used
            if available <= 0:
                continue
            take = min(available, remaining)
            conn.execute(
                "INSERT INTO sale_fifo (sale_item_id, batch_id, qty, cost_price) VALUES (?,?,?,?)",
                (si_id, batch_id, take, cost)
            )
            remaining -= take
            if remaining <= 0:
                break
    conn.commit()

# run recalc
recalc_fifo_for_sale(conn, sale_id)

# show sale_fifo
rows = c.execute("SELECT sale_item_id, batch_id, qty, cost_price FROM sale_fifo").fetchall()
print('sale_fifo rows:', rows)

conn.close()
