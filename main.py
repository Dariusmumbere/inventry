# main.py
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import asyncpg
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="StockMaster UG API",
    description="Backend for SME Inventory System with Offline Capability",
    version="1.0.0"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection pool
pool = None

async def get_db():
    global pool
    if pool is None:
        # Use Render's DATABASE_URL if available, otherwise use .env
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            # Parse the DATABASE_URL into connection parameters
            import urllib.parse
            parsed = urllib.parse.urlparse(database_url)
            pool = await asyncpg.create_pool(
                user=parsed.username,
                password=parsed.password,
                database=parsed.path[1:],  # remove leading '/'
                host=parsed.hostname,
                port=parsed.port
            )
        else:
            # Fallback to individual environment variables
            pool = await asyncpg.create_pool(
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT", 5432)
            )
    return pool

# Models
class Product(BaseModel):
    id: int
    name: str
    category_id: int
    description: Optional[str] = None
    purchase_price: float
    selling_price: float
    stock: int
    reorder_level: int
    unit: str
    barcode: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    is_deleted: bool = False

    class Config:
        from_attributes = True

class Category(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    is_deleted: bool = False

    class Config:
        from_attributes = True

class Supplier(BaseModel):
    id: int
    name: str
    contact_person: Optional[str] = None
    phone: str
    email: Optional[str] = None
    address: Optional[str] = None
    payment_terms: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    is_deleted: bool = False

    class Config:
        from_attributes = True

class SaleItem(BaseModel):
    product_id: int
    product_name: str
    quantity: int
    price: float

class Sale(BaseModel):
    id: int
    date: datetime
    invoice_number: str
    customer: str
    items: List[SaleItem]
    payment_method: str
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    is_deleted: bool = False

    class Config:
        from_attributes = True

class PurchaseItem(BaseModel):
    product_id: int
    product_name: str
    quantity: int
    price: float

class Purchase(BaseModel):
    id: int
    date: datetime
    reference_number: str
    supplier_id: int
    items: List[PurchaseItem]
    payment_method: str
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    is_deleted: bool = False

    class Config:
        from_attributes = True

class Adjustment(BaseModel):
    id: int
    date: datetime
    product_id: int
    type: str  # 'add' or 'remove'
    quantity: int
    reason: str
    user: str
    created_at: datetime
    updated_at: datetime
    is_deleted: bool = False

    class Config:
        from_attributes = True

class Activity(BaseModel):
    id: int
    date: datetime
    activity: str
    user: str
    details: str
    created_at: datetime
    updated_at: datetime
    is_deleted: bool = False

    class Config:
        from_attributes = True

class Settings(BaseModel):
    id: int
    business_name: str
    currency: str
    tax_rate: float
    low_stock_threshold: int
    invoice_prefix: str
    purchase_prefix: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class SyncData(BaseModel):
    last_sync_time: Optional[datetime] = None
    products: List[Product] = []
    categories: List[Category] = []
    suppliers: List[Supplier] = []
    sales: List[Sale] = []
    purchases: List[Purchase] = []
    adjustments: List[Adjustment] = []
    activities: List[Activity] = []
    settings: Optional[Settings] = None

# Database initialization
async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            category_id INTEGER REFERENCES categories(id),
            description TEXT,
            purchase_price DECIMAL(10, 2) NOT NULL,
            selling_price DECIMAL(10, 2) NOT NULL,
            stock INTEGER NOT NULL,
            reorder_level INTEGER NOT NULL,
            unit VARCHAR(50) NOT NULL,
            barcode VARCHAR(100),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_deleted BOOLEAN DEFAULT FALSE
        )
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_deleted BOOLEAN DEFAULT FALSE
        )
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS suppliers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            contact_person VARCHAR(255),
            phone VARCHAR(50) NOT NULL,
            email VARCHAR(255),
            address TEXT,
            payment_terms TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_deleted BOOLEAN DEFAULT FALSE
        )
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS supplier_products (
            supplier_id INTEGER REFERENCES suppliers(id),
            product_id INTEGER REFERENCES products(id),
            PRIMARY KEY (supplier_id, product_id)
        )
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id SERIAL PRIMARY KEY,
            date TIMESTAMP WITH TIME ZONE NOT NULL,
            invoice_number VARCHAR(100) NOT NULL,
            customer VARCHAR(255),
            payment_method VARCHAR(50) NOT NULL,
            notes TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_deleted BOOLEAN DEFAULT FALSE
        )
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS sale_items (
            sale_id INTEGER REFERENCES sales(id),
            product_id INTEGER REFERENCES products(id),
            product_name VARCHAR(255) NOT NULL,
            quantity INTEGER NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            PRIMARY KEY (sale_id, product_id)
        )
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            id SERIAL PRIMARY KEY,
            date TIMESTAMP WITH TIME ZONE NOT NULL,
            reference_number VARCHAR(100) NOT NULL,
            supplier_id INTEGER REFERENCES suppliers(id),
            payment_method VARCHAR(50) NOT NULL,
            notes TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_deleted BOOLEAN DEFAULT FALSE
        )
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS purchase_items (
            purchase_id INTEGER REFERENCES purchases(id),
            product_id INTEGER REFERENCES products(id),
            product_name VARCHAR(255) NOT NULL,
            quantity INTEGER NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            PRIMARY KEY (purchase_id, product_id)
        )
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS adjustments (
            id SERIAL PRIMARY KEY,
            date TIMESTAMP WITH TIME ZONE NOT NULL,
            product_id INTEGER REFERENCES products(id),
            type VARCHAR(10) NOT NULL,  -- 'add' or 'remove'
            quantity INTEGER NOT NULL,
            reason TEXT NOT NULL,
            user VARCHAR(255) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_deleted BOOLEAN DEFAULT FALSE
        )
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS activities (
            id SERIAL PRIMARY KEY,
            date TIMESTAMP WITH TIME ZONE NOT NULL,
            activity VARCHAR(255) NOT NULL,
            user VARCHAR(255) NOT NULL,
            details TEXT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_deleted BOOLEAN DEFAULT FALSE
        )
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            id SERIAL PRIMARY KEY,
            business_name VARCHAR(255) NOT NULL,
            currency VARCHAR(3) NOT NULL,
            tax_rate DECIMAL(5, 2) NOT NULL,
            low_stock_threshold INTEGER NOT NULL,
            invoice_prefix VARCHAR(10) NOT NULL,
            purchase_prefix VARCHAR(10) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """)
        
        # Insert default settings if none exist
        settings = await conn.fetchrow("SELECT * FROM settings LIMIT 1")
        if not settings:
            await conn.execute("""
            INSERT INTO settings (
                business_name, currency, tax_rate, low_stock_threshold, 
                invoice_prefix, purchase_prefix
            ) VALUES (
                'StockMaster UG', 'UGX', 18.0, 5, 'INV', 'PUR'
            )
            """)

@app.on_event("startup")
async def startup():
    await init_db()

@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()

# Helper functions
async def get_products_modified_since(conn, since: datetime = None):
    query = "SELECT * FROM products WHERE is_deleted = FALSE"
    params = []
    if since:
        query += " AND updated_at > $1"
        params.append(since)
    return await conn.fetch(query, *params)

async def get_categories_modified_since(conn, since: datetime = None):
    query = "SELECT * FROM categories WHERE is_deleted = FALSE"
    params = []
    if since:
        query += " AND updated_at > $1"
        params.append(since)
    return await conn.fetch(query, *params)

async def get_suppliers_modified_since(conn, since: datetime = None):
    query = "SELECT * FROM suppliers WHERE is_deleted = FALSE"
    params = []
    if since:
        query += " AND updated_at > $1"
        params.append(since)
    return await conn.fetch(query, *params)

async def get_sales_modified_since(conn, since: datetime = None):
    query = """
    SELECT s.*, 
           json_agg(json_build_object(
               'product_id', si.product_id,
               'product_name', si.product_name,
               'quantity', si.quantity,
               'price', si.price
           )) as items
    FROM sales s
    LEFT JOIN sale_items si ON s.id = si.sale_id
    WHERE s.is_deleted = FALSE
    """
    params = []
    if since:
        query += " AND s.updated_at > $1"
        params.append(since)
    query += " GROUP BY s.id"
    return await conn.fetch(query, *params)

async def get_purchases_modified_since(conn, since: datetime = None):
    query = """
    SELECT p.*, 
           json_agg(json_build_object(
               'product_id', pi.product_id,
               'product_name', pi.product_name,
               'quantity', pi.quantity,
               'price', pi.price
           )) as items
    FROM purchases p
    LEFT JOIN purchase_items pi ON p.id = pi.purchase_id
    WHERE p.is_deleted = FALSE
    """
    params = []
    if since:
        query += " AND p.updated_at > $1"
        params.append(since)
    query += " GROUP BY p.id"
    return await conn.fetch(query, *params)

async def get_adjustments_modified_since(conn, since: datetime = None):
    query = "SELECT * FROM adjustments WHERE is_deleted = FALSE"
    params = []
    if since:
        query += " AND updated_at > $1"
        params.append(since)
    return await conn.fetch(query, *params)

async def get_activities_modified_since(conn, since: datetime = None):
    query = "SELECT * FROM activities WHERE is_deleted = FALSE"
    params = []
    if since:
        query += " AND updated_at > $1"
        params.append(since)
    return await conn.fetch(query, *params)

async def get_settings(conn):
    return await conn.fetchrow("SELECT * FROM settings LIMIT 1")

# API Endpoints
@app.post("/sync", response_model=SyncData)
async def sync_data(sync_request: SyncData, db=Depends(get_db)):
    """
    Synchronize data between client and server.
    Implements a two-way sync with conflict resolution.
    """
    async with db.acquire() as conn:
        async with conn.transaction():
            # Get current server time for last_sync_time
            server_time = await conn.fetchval("SELECT NOW()")
            
            # Process incoming data (client -> server)
            # Products
            for product in sync_request.products:
                existing = await conn.fetchrow(
                    "SELECT * FROM products WHERE id = $1", product.id
                )
                if existing:
                    # Update if client version is newer
                    if product.updated_at > existing['updated_at']:
                        await conn.execute(
                            """
                            UPDATE products SET
                                name = $1,
                                category_id = $2,
                                description = $3,
                                purchase_price = $4,
                                selling_price = $5,
                                stock = $6,
                                reorder_level = $7,
                                unit = $8,
                                barcode = $9,
                                updated_at = NOW(),
                                is_deleted = $10
                            WHERE id = $11
                            """,
                            product.name,
                            product.category_id,
                            product.description,
                            product.purchase_price,
                            product.selling_price,
                            product.stock,
                            product.reorder_level,
                            product.unit,
                            product.barcode,
                            product.is_deleted,
                            product.id
                        )
                else:
                    # Insert new product
                    await conn.execute(
                        """
                        INSERT INTO products (
                            id, name, category_id, description, purchase_price,
                            selling_price, stock, reorder_level, unit, barcode,
                            created_at, updated_at, is_deleted
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
                        )
                        """,
                        product.id,
                        product.name,
                        product.category_id,
                        product.description,
                        product.purchase_price,
                        product.selling_price,
                        product.stock,
                        product.reorder_level,
                        product.unit,
                        product.barcode,
                        product.created_at,
                        product.updated_at,
                        product.is_deleted
                    )
            
            # Categories
            for category in sync_request.categories:
                existing = await conn.fetchrow(
                    "SELECT * FROM categories WHERE id = $1", category.id
                )
                if existing:
                    if category.updated_at > existing['updated_at']:
                        await conn.execute(
                            """
                            UPDATE categories SET
                                name = $1,
                                description = $2,
                                updated_at = NOW(),
                                is_deleted = $3
                            WHERE id = $4
                            """,
                            category.name,
                            category.description,
                            category.is_deleted,
                            category.id
                        )
                else:
                    await conn.execute(
                        """
                        INSERT INTO categories (
                            id, name, description, created_at, updated_at, is_deleted
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6
                        )
                        """,
                        category.id,
                        category.name,
                        category.description,
                        category.created_at,
                        category.updated_at,
                        category.is_deleted
                    )
            
            # Suppliers
            for supplier in sync_request.suppliers:
                existing = await conn.fetchrow(
                    "SELECT * FROM suppliers WHERE id = $1", supplier.id
                )
                if existing:
                    if supplier.updated_at > existing['updated_at']:
                        await conn.execute(
                            """
                            UPDATE suppliers SET
                                name = $1,
                                contact_person = $2,
                                phone = $3,
                                email = $4,
                                address = $5,
                                payment_terms = $6,
                                updated_at = NOW(),
                                is_deleted = $7
                            WHERE id = $8
                            """,
                            supplier.name,
                            supplier.contact_person,
                            supplier.phone,
                            supplier.email,
                            supplier.address,
                            supplier.payment_terms,
                            supplier.is_deleted,
                            supplier.id
                        )
                        # Update supplier products
                        await conn.execute(
                            "DELETE FROM supplier_products WHERE supplier_id = $1", 
                            supplier.id
                        )
                        # Note: The frontend should include product IDs in the supplier object
                else:
                    await conn.execute(
                        """
                        INSERT INTO suppliers (
                            id, name, contact_person, phone, email, address,
                            payment_terms, created_at, updated_at, is_deleted
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                        )
                        """,
                        supplier.id,
                        supplier.name,
                        supplier.contact_person,
                        supplier.phone,
                        supplier.email,
                        supplier.address,
                        supplier.payment_terms,
                        supplier.created_at,
                        supplier.updated_at,
                        supplier.is_deleted
                    )
            
            # Sales
            for sale in sync_request.sales:
                existing = await conn.fetchrow(
                    "SELECT * FROM sales WHERE id = $1", sale.id
                )
                if existing:
                    if sale.updated_at > existing['updated_at']:
                        await conn.execute(
                            """
                            UPDATE sales SET
                                date = $1,
                                invoice_number = $2,
                                customer = $3,
                                payment_method = $4,
                                notes = $5,
                                updated_at = NOW(),
                                is_deleted = $6
                            WHERE id = $7
                            """,
                            sale.date,
                            sale.invoice_number,
                            sale.customer,
                            sale.payment_method,
                            sale.notes,
                            sale.is_deleted,
                            sale.id
                        )
                        # Update sale items
                        await conn.execute(
                            "DELETE FROM sale_items WHERE sale_id = $1", 
                            sale.id
                        )
                        for item in sale.items:
                            await conn.execute(
                                """
                                INSERT INTO sale_items (
                                    sale_id, product_id, product_name, quantity, price
                                ) VALUES (
                                    $1, $2, $3, $4, $5
                                )
                                """,
                                sale.id,
                                item.product_id,
                                item.product_name,
                                item.quantity,
                                item.price
                            )
                else:
                    await conn.execute(
                        """
                        INSERT INTO sales (
                            id, date, invoice_number, customer, payment_method,
                            notes, created_at, updated_at, is_deleted
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9
                        )
                        """,
                        sale.id,
                        sale.date,
                        sale.invoice_number,
                        sale.customer,
                        sale.payment_method,
                        sale.notes,
                        sale.created_at,
                        sale.updated_at,
                        sale.is_deleted
                    )
                    for item in sale.items:
                        await conn.execute(
                            """
                            INSERT INTO sale_items (
                                sale_id, product_id, product_name, quantity, price
                            ) VALUES (
                                $1, $2, $3, $4, $5
                            )
                            """,
                            sale.id,
                            item.product_id,
                            item.product_name,
                            item.quantity,
                            item.price
                        )
            
            # Purchases
            for purchase in sync_request.purchases:
                existing = await conn.fetchrow(
                    "SELECT * FROM purchases WHERE id = $1", purchase.id
                )
                if existing:
                    if purchase.updated_at > existing['updated_at']:
                        await conn.execute(
                            """
                            UPDATE purchases SET
                                date = $1,
                                reference_number = $2,
                                supplier_id = $3,
                                payment_method = $4,
                                notes = $5,
                                updated_at = NOW(),
                                is_deleted = $6
                            WHERE id = $7
                            """,
                            purchase.date,
                            purchase.reference_number,
                            purchase.supplier_id,
                            purchase.payment_method,
                            purchase.notes,
                            purchase.is_deleted,
                            purchase.id
                        )
                        # Update purchase items
                        await conn.execute(
                            "DELETE FROM purchase_items WHERE purchase_id = $1", 
                            purchase.id
                        )
                        for item in purchase.items:
                            await conn.execute(
                                """
                                INSERT INTO purchase_items (
                                    purchase_id, product_id, product_name, quantity, price
                                ) VALUES (
                                    $1, $2, $3, $4, $5
                                )
                                """,
                                purchase.id,
                                item.product_id,
                                item.product_name,
                                item.quantity,
                                item.price
                            )
                else:
                    await conn.execute(
                        """
                        INSERT INTO purchases (
                            id, date, reference_number, supplier_id, payment_method,
                            notes, created_at, updated_at, is_deleted
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9
                        )
                        """,
                        purchase.id,
                        purchase.date,
                        purchase.reference_number,
                        purchase.supplier_id,
                        purchase.payment_method,
                        purchase.notes,
                        purchase.created_at,
                        purchase.updated_at,
                        purchase.is_deleted
                    )
                    for item in purchase.items:
                        await conn.execute(
                            """
                            INSERT INTO purchase_items (
                                purchase_id, product_id, product_name, quantity, price
                            ) VALUES (
                                $1, $2, $3, $4, $5
                            )
                            """,
                            purchase.id,
                            item.product_id,
                            item.product_name,
                            item.quantity,
                            item.price
                        )
            
            # Adjustments
            for adjustment in sync_request.adjustments:
                existing = await conn.fetchrow(
                    "SELECT * FROM adjustments WHERE id = $1", adjustment.id
                )
                if existing:
                    if adjustment.updated_at > existing['updated_at']:
                        await conn.execute(
                            """
                            UPDATE adjustments SET
                                date = $1,
                                product_id = $2,
                                type = $3,
                                quantity = $4,
                                reason = $5,
                                user = $6,
                                updated_at = NOW(),
                                is_deleted = $7
                            WHERE id = $8
                            """,
                            adjustment.date,
                            adjustment.product_id,
                            adjustment.type,
                            adjustment.quantity,
                            adjustment.reason,
                            adjustment.user,
                            adjustment.is_deleted,
                            adjustment.id
                        )
                else:
                    await conn.execute(
                        """
                        INSERT INTO adjustments (
                            id, date, product_id, type, quantity, reason, user,
                            created_at, updated_at, is_deleted
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                        )
                        """,
                        adjustment.id,
                        adjustment.date,
                        adjustment.product_id,
                        adjustment.type,
                        adjustment.quantity,
                        adjustment.reason,
                        adjustment.user,
                        adjustment.created_at,
                        adjustment.updated_at,
                        adjustment.is_deleted
                    )
            
            # Activities
            for activity in sync_request.activities:
                existing = await conn.fetchrow(
                    "SELECT * FROM activities WHERE id = $1", activity.id
                )
                if existing:
                    if activity.updated_at > existing['updated_at']:
                        await conn.execute(
                            """
                            UPDATE activities SET
                                date = $1,
                                activity = $2,
                                user = $3,
                                details = $4,
                                updated_at = NOW(),
                                is_deleted = $5
                            WHERE id = $6
                            """,
                            activity.date,
                            activity.activity,
                            activity.user,
                            activity.details,
                            activity.is_deleted,
                            activity.id
                        )
                else:
                    await conn.execute(
                        """
                        INSERT INTO activities (
                            id, date, activity, user, details, created_at, updated_at, is_deleted
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8
                        )
                        """,
                        activity.id,
                        activity.date,
                        activity.activity,
                        activity.user,
                        activity.details,
                        activity.created_at,
                        activity.updated_at,
                        activity.is_deleted
                    )
            
            # Settings
            if sync_request.settings:
                existing_settings = await conn.fetchrow("SELECT * FROM settings LIMIT 1")
                if existing_settings:
                    await conn.execute(
                        """
                        UPDATE settings SET
                            business_name = $1,
                            currency = $2,
                            tax_rate = $3,
                            low_stock_threshold = $4,
                            invoice_prefix = $5,
                            purchase_prefix = $6,
                            updated_at = NOW()
                        WHERE id = $7
                        """,
                        sync_request.settings.business_name,
                        sync_request.settings.currency,
                        sync_request.settings.tax_rate,
                        sync_request.settings.low_stock_threshold,
                        sync_request.settings.invoice_prefix,
                        sync_request.settings.purchase_prefix,
                        existing_settings['id']
                    )
                else:
                    await conn.execute(
                        """
                        INSERT INTO settings (
                            business_name, currency, tax_rate, low_stock_threshold,
                            invoice_prefix, purchase_prefix, created_at, updated_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, NOW(), NOW()
                        )
                        """,
                        sync_request.settings.business_name,
                        sync_request.settings.currency,
                        sync_request.settings.tax_rate,
                        sync_request.settings.low_stock_threshold,
                        sync_request.settings.invoice_prefix,
                        sync_request.settings.purchase_prefix
                    )
            
            # Get data modified since last sync (server -> client)
            last_sync_time = sync_request.last_sync_time
            products = await get_products_modified_since(conn, last_sync_time)
            categories = await get_categories_modified_since(conn, last_sync_time)
            suppliers = await get_suppliers_modified_since(conn, last_sync_time)
            sales = await get_sales_modified_since(conn, last_sync_time)
            purchases = await get_purchases_modified_since(conn, last_sync_time)
            adjustments = await get_adjustments_modified_since(conn, last_sync_time)
            activities = await get_activities_modified_since(conn, last_sync_time)
            settings = await get_settings(conn)
            
            return {
                "last_sync_time": server_time,
                "products": products,
                "categories": categories,
                "suppliers": suppliers,
                "sales": sales,
                "purchases": purchases,
                "adjustments": adjustments,
                "activities": activities,
                "settings": settings
            }

# Additional endpoints for direct API access (optional)
@app.get("/products", response_model=List[Product])
async def get_products(db=Depends(get_db)):
    return await db.fetch("SELECT * FROM products WHERE is_deleted = FALSE")

@app.get("/categories", response_model=List[Category])
async def get_categories(db=Depends(get_db)):
    return await db.fetch("SELECT * FROM categories WHERE is_deleted = FALSE")

@app.get("/suppliers", response_model=List[Supplier])
async def get_suppliers(db=Depends(get_db)):
    return await db.fetch("SELECT * FROM suppliers WHERE is_deleted = FALSE")

@app.get("/sales", response_model=List[Sale])
async def get_sales(db=Depends(get_db)):
    sales = await db.fetch("""
        SELECT s.*, 
               json_agg(json_build_object(
                   'product_id', si.product_id,
                   'product_name', si.product_name,
                   'quantity', si.quantity,
                   'price', si.price
               )) as items
        FROM sales s
        LEFT JOIN sale_items si ON s.id = si.sale_id
        WHERE s.is_deleted = FALSE
        GROUP BY s.id
    """)
    return sales

@app.get("/purchases", response_model=List[Purchase])
async def get_purchases(db=Depends(get_db)):
    purchases = await db.fetch("""
        SELECT p.*, 
               json_agg(json_build_object(
                   'product_id', pi.product_id,
                   'product_name', pi.product_name,
                   'quantity', pi.quantity,
                   'price', pi.price
               )) as items
        FROM purchases p
        LEFT JOIN purchase_items pi ON p.id = pi.purchase_id
        WHERE p.is_deleted = FALSE
        GROUP BY p.id
    """)
    return purchases

@app.get("/adjustments", response_model=List[Adjustment])
async def get_adjustments(db=Depends(get_db)):
    return await db.fetch("SELECT * FROM adjustments WHERE is_deleted = FALSE")

@app.get("/activities", response_model=List[Activity])
async def get_activities(db=Depends(get_db)):
    return await db.fetch("SELECT * FROM activities WHERE is_deleted = FALSE")

@app.get("/settings", response_model=Settings)
async def get_settings_endpoint(db=Depends(get_db)):
    settings = await db.fetchrow("SELECT * FROM settings LIMIT 1")
    if not settings:
        raise HTTPException(status_code=404, detail="Settings not found")
    return settings

# Dashboard endpoints
@app.get("/dashboard/stats")
async def get_dashboard_stats(db=Depends(get_db)):
    async with db.acquire() as conn:
        total_products = await conn.fetchval(
            "SELECT COUNT(*) FROM products WHERE is_deleted = FALSE"
        )
        low_stock_items = await conn.fetchval(
            """
            SELECT COUNT(*) FROM products 
            WHERE is_deleted = FALSE AND stock > 0 AND stock <= reorder_level
            """
        )
        out_of_stock_items = await conn.fetchval(
            "SELECT COUNT(*) FROM products WHERE is_deleted = FALSE AND stock = 0"
        )
        inventory_value = await conn.fetchval(
            "SELECT COALESCE(SUM(stock * purchase_price), 0) FROM products WHERE is_deleted = FALSE"
        )
        
        # Get recent activities
        recent_activities = await conn.fetch(
            "SELECT * FROM activities WHERE is_deleted = FALSE ORDER BY date DESC LIMIT 5"
        )
        
        # Get low stock items
        low_stock_products = await conn.fetch(
            """
            SELECT p.*, c.name as category_name 
            FROM products p
            LEFT JOIN categories c ON p.category_id = c.id
            WHERE p.is_deleted = FALSE AND p.stock <= p.reorder_level
            ORDER BY p.stock ASC
            LIMIT 5
            """
        )
        
        # Get recent sales
        recent_sales = await conn.fetch(
            """
            SELECT s.*, 
                   SUM(si.quantity * si.price) as total_amount
            FROM sales s
            LEFT JOIN sale_items si ON s.id = si.sale_id
            WHERE s.is_deleted = FALSE
            GROUP BY s.id
            ORDER BY s.date DESC
            LIMIT 5
            """
        )
        
        return {
            "total_products": total_products,
            "low_stock_items": low_stock_items,
            "out_of_stock_items": out_of_stock_items,
            "inventory_value": float(inventory_value),
            "recent_activities": recent_activities,
            "low_stock_products": low_stock_products,
            "recent_sales": recent_sales
        }
