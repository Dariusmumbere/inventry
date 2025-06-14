# main.py
import os
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import asyncpg
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI(
    title="StockMaster UG API",
    description="Backend API for StockMaster UG Inventory System",
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

async def get_db_connection():
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
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
    last_synced: Optional[datetime] = None
    is_deleted: bool = False

class Category(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    last_synced: Optional[datetime] = None
    is_deleted: bool = False

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
    last_synced: Optional[datetime] = None
    is_deleted: bool = False

class SaleItem(BaseModel):
    product_id: int
    product_name: str
    quantity: int
    price: float

class Sale(BaseModel):
    id: int
    date: datetime
    invoice_number: str
    customer: Optional[str] = None
    items: List[SaleItem]
    payment_method: str
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    last_synced: Optional[datetime] = None
    is_deleted: bool = False

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
    last_synced: Optional[datetime] = None
    is_deleted: bool = False

class StockAdjustment(BaseModel):
    id: int
    date: datetime
    product_id: int
    type: str  # 'add' or 'remove'
    quantity: int
    reason: str
    user: str
    created_at: datetime
    updated_at: datetime
    last_synced: Optional[datetime] = None
    is_deleted: bool = False

class Activity(BaseModel):
    id: int
    date: datetime
    activity: str
    user: str
    details: str
    created_at: datetime
    last_synced: Optional[datetime] = None
    is_deleted: bool = False

class Settings(BaseModel):
    business_name: str
    currency: str
    tax_rate: float
    low_stock_threshold: int
    invoice_prefix: str
    purchase_prefix: str
    updated_at: datetime
    last_synced: Optional[datetime] = None

# Request models
class ProductCreate(BaseModel):
    name: str
    category_id: int
    description: Optional[str] = None
    purchase_price: float
    selling_price: float
    stock: int
    reorder_level: int
    unit: str
    barcode: Optional[str] = None

class CategoryCreate(BaseModel):
    name: str
    description: Optional[str] = None

class SupplierCreate(BaseModel):
    name: str
    contact_person: Optional[str] = None
    phone: str
    email: Optional[str] = None
    address: Optional[str] = None
    payment_terms: Optional[str] = None

class SaleCreate(BaseModel):
    date: datetime
    customer: Optional[str] = None
    items: List[SaleItem]
    payment_method: str
    notes: Optional[str] = None

class PurchaseCreate(BaseModel):
    date: datetime
    supplier_id: int
    items: List[PurchaseItem]
    payment_method: str
    notes: Optional[str] = None

class StockAdjustmentCreate(BaseModel):
    product_id: int
    type: str
    quantity: int
    reason: str
    user: str

class SyncRequest(BaseModel):
    last_sync_time: Optional[datetime] = None
    products: List[Product] = []
    categories: List[Category] = []
    suppliers: List[Supplier] = []
    sales: List[Sale] = []
    purchases: List[Purchase] = []
    adjustments: List[StockAdjustment] = []
    activities: List[Activity] = []
    settings: Optional[Settings] = None

class SyncResponse(BaseModel):
    server_time: datetime
    products: List[Product] = []
    categories: List[Category] = []
    suppliers: List[Supplier] = []
    sales: List[Sale] = []
    purchases: List[Purchase] = []
    adjustments: List[StockAdjustment] = []
    activities: List[Activity] = []
    settings: Optional[Settings] = None

# Database initialization
async def init_db():
    conn = await get_db_connection()
    async with conn.acquire() as connection:
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                category_id INTEGER NOT NULL,
                description TEXT,
                purchase_price DECIMAL(10, 2) NOT NULL,
                selling_price DECIMAL(10, 2) NOT NULL,
                stock INTEGER NOT NULL,
                reorder_level INTEGER NOT NULL,
                unit VARCHAR(50) NOT NULL,
                barcode VARCHAR(100),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                last_synced TIMESTAMP WITH TIME ZONE,
                is_deleted BOOLEAN DEFAULT FALSE
            )
        """)
        
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                last_synced TIMESTAMP WITH TIME ZONE,
                is_deleted BOOLEAN DEFAULT FALSE
            )
        """)
        
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS suppliers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                contact_person VARCHAR(255),
                phone VARCHAR(50) NOT NULL,
                email VARCHAR(255),
                address TEXT,
                payment_terms VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                last_synced TIMESTAMP WITH TIME ZONE,
                is_deleted BOOLEAN DEFAULT FALSE
            )
        """)
        
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP WITH TIME ZONE NOT NULL,
                invoice_number VARCHAR(100) NOT NULL,
                customer VARCHAR(255),
                payment_method VARCHAR(50) NOT NULL,
                notes TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                last_synced TIMESTAMP WITH TIME ZONE,
                is_deleted BOOLEAN DEFAULT FALSE
            )
        """)
        
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS sale_items (
                sale_id INTEGER NOT NULL REFERENCES sales(id),
                product_id INTEGER NOT NULL REFERENCES products(id),
                product_name VARCHAR(255) NOT NULL,
                quantity INTEGER NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                PRIMARY KEY (sale_id, product_id)
            )
        """)
        
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS purchases (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP WITH TIME ZONE NOT NULL,
                reference_number VARCHAR(100) NOT NULL,
                supplier_id INTEGER NOT NULL REFERENCES suppliers(id),
                payment_method VARCHAR(50) NOT NULL,
                notes TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                last_synced TIMESTAMP WITH TIME ZONE,
                is_deleted BOOLEAN DEFAULT FALSE
            )
        """)
        
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS purchase_items (
                purchase_id INTEGER NOT NULL REFERENCES purchases(id),
                product_id INTEGER NOT NULL REFERENCES products(id),
                product_name VARCHAR(255) NOT NULL,
                quantity INTEGER NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                PRIMARY KEY (purchase_id, product_id)
            )
        """)
        
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS stock_adjustments (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP WITH TIME ZONE NOT NULL,
                product_id INTEGER NOT NULL REFERENCES products(id),
                type VARCHAR(10) NOT NULL,
                quantity INTEGER NOT NULL,
                reason TEXT NOT NULL,
                user VARCHAR(255) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                last_synced TIMESTAMP WITH TIME ZONE,
                is_deleted BOOLEAN DEFAULT FALSE
            )
        """)
        
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS activities (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP WITH TIME ZONE NOT NULL,
                activity VARCHAR(255) NOT NULL,
                user VARCHAR(255) NOT NULL,
                details TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                last_synced TIMESTAMP WITH TIME ZONE,
                is_deleted BOOLEAN DEFAULT FALSE
            )
        """)
        
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                id SERIAL PRIMARY KEY,
                business_name VARCHAR(255) NOT NULL,
                currency VARCHAR(10) NOT NULL,
                tax_rate DECIMAL(5, 2) NOT NULL,
                low_stock_threshold INTEGER NOT NULL,
                invoice_prefix VARCHAR(10) NOT NULL,
                purchase_prefix VARCHAR(10) NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                last_synced TIMESTAMP WITH TIME ZONE
            )
        """)
        
        # Insert default settings if none exist
        settings_count = await connection.fetchval("SELECT COUNT(*) FROM settings")
        if settings_count == 0:
            await connection.execute("""
                INSERT INTO settings (
                    business_name, currency, tax_rate, low_stock_threshold, 
                    invoice_prefix, purchase_prefix
                ) VALUES (
                    'StockMaster UG', 'UGX', 18, 5, 'INV', 'PUR'
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
async def get_products(conn, last_sync_time=None, include_deleted=False):
    query = "SELECT * FROM products"
    conditions = []
    
    if last_sync_time:
        conditions.append(f"updated_at > '{last_sync_time.isoformat()}'")
    
    if not include_deleted:
        conditions.append("is_deleted = FALSE")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    return await conn.fetch(query)

async def get_categories(conn, last_sync_time=None, include_deleted=False):
    query = "SELECT * FROM categories"
    conditions = []
    
    if last_sync_time:
        conditions.append(f"updated_at > '{last_sync_time.isoformat()}'")
    
    if not include_deleted:
        conditions.append("is_deleted = FALSE")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    return await conn.fetch(query)

async def get_suppliers(conn, last_sync_time=None, include_deleted=False):
    query = "SELECT * FROM suppliers"
    conditions = []
    
    if last_sync_time:
        conditions.append(f"updated_at > '{last_sync_time.isoformat()}'")
    
    if not include_deleted:
        conditions.append("is_deleted = FALSE")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    return await conn.fetch(query)

async def get_sales(conn, last_sync_time=None, include_deleted=False):
    query = "SELECT * FROM sales"
    conditions = []
    
    if last_sync_time:
        conditions.append(f"updated_at > '{last_sync_time.isoformat()}'")
    
    if not include_deleted:
        conditions.append("is_deleted = FALSE")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    sales = await conn.fetch(query)
    
    for sale in sales:
        sale['items'] = await conn.fetch(
            "SELECT product_id, product_name, quantity, price FROM sale_items WHERE sale_id = $1",
            sale['id']
        )
    
    return sales

async def get_purchases(conn, last_sync_time=None, include_deleted=False):
    query = "SELECT * FROM purchases"
    conditions = []
    
    if last_sync_time:
        conditions.append(f"updated_at > '{last_sync_time.isoformat()}'")
    
    if not include_deleted:
        conditions.append("is_deleted = FALSE")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    purchases = await conn.fetch(query)
    
    for purchase in purchases:
        purchase['items'] = await conn.fetch(
            "SELECT product_id, product_name, quantity, price FROM purchase_items WHERE purchase_id = $1",
            purchase['id']
        )
    
    return purchases

async def get_adjustments(conn, last_sync_time=None, include_deleted=False):
    query = "SELECT * FROM stock_adjustments"
    conditions = []
    
    if last_sync_time:
        conditions.append(f"updated_at > '{last_sync_time.isoformat()}'")
    
    if not include_deleted:
        conditions.append("is_deleted = FALSE")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    return await conn.fetch(query)

async def get_activities(conn, last_sync_time=None, include_deleted=False):
    query = "SELECT * FROM activities"
    conditions = []
    
    if last_sync_time:
        conditions.append(f"created_at > '{last_sync_time.isoformat()}'")
    
    if not include_deleted:
        conditions.append("is_deleted = FALSE")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    return await conn.fetch(query)

async def get_settings(conn, last_sync_time=None):
    query = "SELECT * FROM settings"
    
    if last_sync_time:
        query += f" WHERE updated_at > '{last_sync_time.isoformat()}'"
    
    return await conn.fetchrow(query)

async def update_last_sync_time(conn, table_name, record_id, sync_time):
    await conn.execute(
        f"UPDATE {table_name} SET last_synced = $1 WHERE id = $2",
        sync_time, record_id
    )

# API Endpoints
@app.get("/")
async def root():
    return {"message": "StockMaster UG API is running"}

@app.post("/sync", response_model=SyncResponse)
async def sync_data(request: SyncRequest):
    conn = await get_db_connection()
    async with conn.acquire() as connection:
        async with connection.transaction():
            server_time = datetime.utcnow()
            
            # Process client updates
            for product in request.products:
                existing = await connection.fetchrow("SELECT * FROM products WHERE id = $1", product.id)
                if existing:
                    # Update existing product
                    await connection.execute("""
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
                            updated_at = $10,
                            is_deleted = $11,
                            last_synced = $12
                        WHERE id = $13
                    """, 
                    product.name, product.category_id, product.description, 
                    product.purchase_price, product.selling_price, product.stock,
                    product.reorder_level, product.unit, product.barcode,
                    server_time, product.is_deleted, server_time, product.id)
                else:
                    # Insert new product
                    await connection.execute("""
                        INSERT INTO products (
                            id, name, category_id, description, purchase_price, 
                            selling_price, stock, reorder_level, unit, barcode,
                            created_at, updated_at, is_deleted, last_synced
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
                        )
                    """, 
                    product.id, product.name, product.category_id, product.description,
                    product.purchase_price, product.selling_price, product.stock,
                    product.reorder_level, product.unit, product.barcode,
                    product.created_at, server_time, product.is_deleted, server_time)
            
            for category in request.categories:
                existing = await connection.fetchrow("SELECT * FROM categories WHERE id = $1", category.id)
                if existing:
                    # Update existing category
                    await connection.execute("""
                        UPDATE categories SET
                            name = $1,
                            description = $2,
                            updated_at = $3,
                            is_deleted = $4,
                            last_synced = $5
                        WHERE id = $6
                    """, 
                    category.name, category.description, server_time, 
                    category.is_deleted, server_time, category.id)
                else:
                    # Insert new category
                    await connection.execute("""
                        INSERT INTO categories (
                            id, name, description, created_at, updated_at, 
                            is_deleted, last_synced
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7
                        )
                    """, 
                    category.id, category.name, category.description,
                    category.created_at, server_time, category.is_deleted, server_time)
            
            for supplier in request.suppliers:
                existing = await connection.fetchrow("SELECT * FROM suppliers WHERE id = $1", supplier.id)
                if existing:
                    # Update existing supplier
                    await connection.execute("""
                        UPDATE suppliers SET
                            name = $1,
                            contact_person = $2,
                            phone = $3,
                            email = $4,
                            address = $5,
                            payment_terms = $6,
                            updated_at = $7,
                            is_deleted = $8,
                            last_synced = $9
                        WHERE id = $10
                    """, 
                    supplier.name, supplier.contact_person, supplier.phone,
                    supplier.email, supplier.address, supplier.payment_terms,
                    server_time, supplier.is_deleted, server_time, supplier.id)
                else:
                    # Insert new supplier
                    await connection.execute("""
                        INSERT INTO suppliers (
                            id, name, contact_person, phone, email, address,
                            payment_terms, created_at, updated_at, is_deleted, last_synced
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
                        )
                    """, 
                    supplier.id, supplier.name, supplier.contact_person, supplier.phone,
                    supplier.email, supplier.address, supplier.payment_terms,
                    supplier.created_at, server_time, supplier.is_deleted, server_time)
            
            for sale in request.sales:
                existing = await connection.fetchrow("SELECT * FROM sales WHERE id = $1", sale.id)
                if existing:
                    # Update existing sale
                    await connection.execute("""
                        UPDATE sales SET
                            date = $1,
                            invoice_number = $2,
                            customer = $3,
                            payment_method = $4,
                            notes = $5,
                            updated_at = $6,
                            is_deleted = $7,
                            last_synced = $8
                        WHERE id = $9
                    """, 
                    sale.date, sale.invoice_number, sale.customer,
                    sale.payment_method, sale.notes, server_time,
                    sale.is_deleted, server_time, sale.id)
                    
                    # Update sale items if not deleted
                    if not sale.is_deleted:
                        await connection.execute("DELETE FROM sale_items WHERE sale_id = $1", sale.id)
                        for item in sale.items:
                            await connection.execute("""
                                INSERT INTO sale_items (
                                    sale_id, product_id, product_name, quantity, price
                                ) VALUES (
                                    $1, $2, $3, $4, $5
                                )
                            """, sale.id, item.product_id, item.product_name, item.quantity, item.price)
                else:
                    # Insert new sale
                    await connection.execute("""
                        INSERT INTO sales (
                            id, date, invoice_number, customer, payment_method,
                            notes, created_at, updated_at, is_deleted, last_synced
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                        )
                    """, 
                    sale.id, sale.date, sale.invoice_number, sale.customer,
                    sale.payment_method, sale.notes, sale.created_at,
                    server_time, sale.is_deleted, server_time)
                    
                    # Insert sale items if not deleted
                    if not sale.is_deleted:
                        for item in sale.items:
                            await connection.execute("""
                                INSERT INTO sale_items (
                                    sale_id, product_id, product_name, quantity, price
                                ) VALUES (
                                    $1, $2, $3, $4, $5
                                )
                            """, sale.id, item.product_id, item.product_name, item.quantity, item.price)
            
            for purchase in request.purchases:
                existing = await connection.fetchrow("SELECT * FROM purchases WHERE id = $1", purchase.id)
                if existing:
                    # Update existing purchase
                    await connection.execute("""
                        UPDATE purchases SET
                            date = $1,
                            reference_number = $2,
                            supplier_id = $3,
                            payment_method = $4,
                            notes = $5,
                            updated_at = $6,
                            is_deleted = $7,
                            last_synced = $8
                        WHERE id = $9
                    """, 
                    purchase.date, purchase.reference_number, purchase.supplier_id,
                    purchase.payment_method, purchase.notes, server_time,
                    purchase.is_deleted, server_time, purchase.id)
                    
                    # Update purchase items if not deleted
                    if not purchase.is_deleted:
                        await connection.execute("DELETE FROM purchase_items WHERE purchase_id = $1", purchase.id)
                        for item in purchase.items:
                            await connection.execute("""
                                INSERT INTO purchase_items (
                                    purchase_id, product_id, product_name, quantity, price
                                ) VALUES (
                                    $1, $2, $3, $4, $5
                                )
                            """, purchase.id, item.product_id, item.product_name, item.quantity, item.price)
                else:
                    # Insert new purchase
                    await connection.execute("""
                        INSERT INTO purchases (
                            id, date, reference_number, supplier_id, payment_method,
                            notes, created_at, updated_at, is_deleted, last_synced
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                        )
                    """, 
                    purchase.id, purchase.date, purchase.reference_number, purchase.supplier_id,
                    purchase.payment_method, purchase.notes, purchase.created_at,
                    server_time, purchase.is_deleted, server_time)
                    
                    # Insert purchase items if not deleted
                    if not purchase.is_deleted:
                        for item in purchase.items:
                            await connection.execute("""
                                INSERT INTO purchase_items (
                                    purchase_id, product_id, product_name, quantity, price
                                ) VALUES (
                                    $1, $2, $3, $4, $5
                                )
                            """, purchase.id, item.product_id, item.product_name, item.quantity, item.price)
            
            for adjustment in request.adjustments:
                existing = await connection.fetchrow("SELECT * FROM stock_adjustments WHERE id = $1", adjustment.id)
                if existing:
                    # Update existing adjustment
                    await connection.execute("""
                        UPDATE stock_adjustments SET
                            date = $1,
                            product_id = $2,
                            type = $3,
                            quantity = $4,
                            reason = $5,
                            user = $6,
                            updated_at = $7,
                            is_deleted = $8,
                            last_synced = $9
                        WHERE id = $10
                    """, 
                    adjustment.date, adjustment.product_id, adjustment.type,
                    adjustment.quantity, adjustment.reason, adjustment.user,
                    server_time, adjustment.is_deleted, server_time, adjustment.id)
                else:
                    # Insert new adjustment
                    await connection.execute("""
                        INSERT INTO stock_adjustments (
                            id, date, product_id, type, quantity, reason, user,
                            created_at, updated_at, is_deleted, last_synced
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
                        )
                    """, 
                    adjustment.id, adjustment.date, adjustment.product_id, adjustment.type,
                    adjustment.quantity, adjustment.reason, adjustment.user,
                    adjustment.created_at, server_time, adjustment.is_deleted, server_time)
            
            for activity in request.activities:
                existing = await connection.fetchrow("SELECT * FROM activities WHERE id = $1", activity.id)
                if existing:
                    # Update existing activity
                    await connection.execute("""
                        UPDATE activities SET
                            date = $1,
                            activity = $2,
                            user = $3,
                            details = $4,
                            is_deleted = $5,
                            last_synced = $6
                        WHERE id = $7
                    """, 
                    activity.date, activity.activity, activity.user,
                    activity.details, activity.is_deleted, server_time, activity.id)
                else:
                    # Insert new activity
                    await connection.execute("""
                        INSERT INTO activities (
                            id, date, activity, user, details, created_at, 
                            is_deleted, last_synced
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8
                        )
                    """, 
                    activity.id, activity.date, activity.activity, activity.user,
                    activity.details, activity.created_at, activity.is_deleted, server_time)
            
            if request.settings:
                await connection.execute("""
                    UPDATE settings SET
                        business_name = $1,
                        currency = $2,
                        tax_rate = $3,
                        low_stock_threshold = $4,
                        invoice_prefix = $5,
                        purchase_prefix = $6,
                        updated_at = $7,
                        last_synced = $8
                """, 
                request.settings.business_name, request.settings.currency,
                request.settings.tax_rate, request.settings.low_stock_threshold,
                request.settings.invoice_prefix, request.settings.purchase_prefix,
                server_time, server_time)
            
            # Get server changes since last sync
            products = await get_products(connection, request.last_sync_time)
            categories = await get_categories(connection, request.last_sync_time)
            suppliers = await get_suppliers(connection, request.last_sync_time)
            sales = await get_sales(connection, request.last_sync_time)
            purchases = await get_purchases(connection, request.last_sync_time)
            adjustments = await get_adjustments(connection, request.last_sync_time)
            activities = await get_activities(connection, request.last_sync_time)
            settings = await get_settings(connection, request.last_sync_time)
            
            return SyncResponse(
                server_time=server_time,
                products=products,
                categories=categories,
                suppliers=suppliers,
                sales=sales,
                purchases=purchases,
                adjustments=adjustments,
                activities=activities,
                settings=settings
            )

# Error handling
@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"message": "An unexpected error occurred", "detail": str(exc)}
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"message": exc.detail}
    )
