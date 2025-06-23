from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import asyncpg
import os
from dotenv import load_dotenv
import logging
import json
from fastapi.encoders import jsonable_encoder

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="StockMaster UG Inventory API",
    description="Backend API for SME Inventory System",
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
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/inventory")
pool = None

async def get_db():
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(DATABASE_URL)
    return pool

# Helper function to ensure timezone-naive datetimes
def make_timezone_naive(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

# Models with proper datetime handling
class Product(BaseModel):
    id: int
    name: str
    category_id: Optional[int] = None
    description: Optional[str] = None
    purchase_price: float
    selling_price: float
    stock: int
    reorder_level: int
    unit: str
    barcode: Optional[str] = None
    created_at: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class Category(BaseModel):
    id: int
    name: str
    description: Optional[str] = None

class Supplier(BaseModel):
    id: int
    name: str
    contact_person: Optional[str] = None
    phone: str
    email: Optional[str] = None
    address: Optional[str] = None
    products: List[int] = []
    payment_terms: Optional[str] = None

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

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

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

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class Adjustment(BaseModel):
    id: int
    date: datetime
    product_id: int
    type: str  # 'add' or 'remove'
    quantity: int
    reason: str
    username: Optional[str] = "system"

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class Activity(BaseModel):
    id: int
    date: datetime
    activity: str
    username: str = "system"  # Changed from Optional to required with default
    details: str

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class Settings(BaseModel):
    business_name: str
    currency: str
    tax_rate: float
    low_stock_threshold: int
    invoice_prefix: str
    purchase_prefix: str

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

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

# Database initialization with proper timestamp handling
async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        # Check and create tables with all required columns
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                category_id INTEGER,
                description TEXT,
                purchase_price DECIMAL(10, 2) NOT NULL,
                selling_price DECIMAL(10, 2) NOT NULL,
                stock INTEGER NOT NULL,
                reorder_level INTEGER NOT NULL,
                unit TEXT NOT NULL,
                barcode TEXT,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS suppliers (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                contact_person TEXT,
                phone TEXT NOT NULL,
                email TEXT,
                address TEXT,
                products INTEGER[] DEFAULT '{}',
                payment_terms TEXT
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS sales (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                invoice_number TEXT NOT NULL,
                customer TEXT,
                items JSONB NOT NULL,
                payment_method TEXT NOT NULL,
                notes TEXT
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                reference_number TEXT NOT NULL,
                supplier_id INTEGER,
                items JSONB NOT NULL,
                payment_method TEXT NOT NULL,
                notes TEXT
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS adjustments (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                product_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                reason TEXT NOT NULL,
                username TEXT NOT NULL DEFAULT 'system'
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS activities (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                activity TEXT NOT NULL,
                username TEXT NOT NULL DEFAULT 'system',
                details TEXT NOT NULL
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                business_name TEXT NOT NULL,
                currency TEXT NOT NULL,
                tax_rate DECIMAL(5, 2) NOT NULL,
                low_stock_threshold INTEGER NOT NULL,
                invoice_prefix TEXT NOT NULL,
                purchase_prefix TEXT NOT NULL,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Insert default settings if none exist
        settings = await conn.fetchrow('SELECT * FROM settings LIMIT 1')
        if not settings:
            await conn.execute('''
                INSERT INTO settings (
                    business_name, currency, tax_rate, 
                    low_stock_threshold, invoice_prefix, purchase_prefix
                ) VALUES ($1, $2, $3, $4, $5, $6)
            ''', 'StockMaster UG', 'UGX', 18, 5, 'INV', 'PUR')

@app.on_event("startup")
async def startup():
    await init_db()
    logger.info("Database initialized")

@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()
        logger.info("Database connection pool closed")

# Helper functions for data conversion with proper datetime handling
def record_to_product(record) -> Product:
    return Product(
        id=record['id'],
        name=record['name'],
        category_id=record['category_id'],
        description=record['description'],
        purchase_price=record['purchase_price'],
        selling_price=record['selling_price'],
        stock=record['stock'],
        reorder_level=record['reorder_level'],
        unit=record['unit'],
        barcode=record['barcode'],
        created_at=make_timezone_naive(record['created_at'])
    )

def record_to_category(record) -> Category:
    return Category(
        id=record['id'],
        name=record['name'],
        description=record['description']
    )

def record_to_supplier(record) -> Supplier:
    return Supplier(
        id=record['id'],
        name=record['name'],
        contact_person=record['contact_person'],
        phone=record['phone'],
        email=record['email'],
        address=record['address'],
        products=record['products'],
        payment_terms=record['payment_terms']
    )

def record_to_sale(record) -> Sale:
    items = [SaleItem(**item) for item in record['items']]
    return Sale(
        id=record['id'],
        date=make_timezone_naive(record['date']),
        invoice_number=record['invoice_number'],
        customer=record['customer'],
        items=items,
        payment_method=record['payment_method'],
        notes=record['notes']
    )

def record_to_purchase(record) -> Purchase:
    items = [PurchaseItem(**item) for item in record['items']]
    return Purchase(
        id=record['id'],
        date=make_timezone_naive(record['date']),
        reference_number=record['reference_number'],
        supplier_id=record['supplier_id'],
        items=items,
        payment_method=record['payment_method'],
        notes=record['notes']
    )

def record_to_adjustment(record) -> Adjustment:
    return Adjustment(
        id=record['id'],
        date=make_timezone_naive(record['date']),
        product_id=record['product_id'],
        type=record['type'],
        quantity=record['quantity'],
        reason=record['reason'],
        username=record.get('username', 'system')
    )

def record_to_activity(record) -> Activity:
    return Activity(
        id=record['id'],
        date=make_timezone_naive(record['date']),
        activity=record['activity'],
        username=record.get('username', 'system'),
        details=record['details']
    )

def record_to_settings(record) -> Settings:
    return Settings(
        business_name=record['business_name'],
        currency=record['currency'],
        tax_rate=record['tax_rate'],
        low_stock_threshold=record['low_stock_threshold'],
        invoice_prefix=record['invoice_prefix'],
        purchase_prefix=record['purchase_prefix']
    )

# Sync endpoint with proper datetime handling
@app.post("/sync", response_model=SyncData)
async def sync(data: Dict[str, Any], db=Depends(get_db)):
    try:
        # Validate and parse the incoming data with proper datetime handling
        sync_data = SyncData(**data)
        server_time = datetime.now(timezone.utc).replace(tzinfo=None)  # Timezone-naive datetime
        
        logger.info(f"Received sync data with {len(sync_data.products)} products, "
                   f"{len(sync_data.categories)} categories, "
                   f"{len(sync_data.sales)} sales")
        
        result = SyncData(last_sync_time=server_time)
        
        async with db.acquire() as conn:
            # Process products
            for product in sync_data.products:
                existing = await conn.fetchrow('SELECT * FROM products WHERE id = $1', product.id)
                if existing:
                    await conn.execute('''
                        UPDATE products SET 
                            name = $1, category_id = $2, description = $3,
                            purchase_price = $4, selling_price = $5, stock = $6,
                            reorder_level = $7, unit = $8, barcode = $9
                        WHERE id = $10
                    ''', product.name, product.category_id, product.description,
                        product.purchase_price, product.selling_price, product.stock,
                        product.reorder_level, product.unit, product.barcode, product.id)
                else:
                    await conn.execute('''
                        INSERT INTO products (
                            id, name, category_id, description, purchase_price,
                            selling_price, stock, reorder_level, unit, barcode, created_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ''', product.id, product.name, product.category_id, product.description,
                        product.purchase_price, product.selling_price, product.stock,
                        product.reorder_level, product.unit, product.barcode, 
                        make_timezone_naive(product.created_at) or server_time)
                result.products.append(product)
            
            # Process categories
            for category in sync_data.categories:
                existing = await conn.fetchrow('SELECT * FROM categories WHERE id = $1', category.id)
                if existing:
                    await conn.execute('''
                        UPDATE categories SET name = $1, description = $2 WHERE id = $3
                    ''', category.name, category.description, category.id)
                else:
                    await conn.execute('''
                        INSERT INTO categories (id, name, description) VALUES ($1, $2, $3)
                    ''', category.id, category.name, category.description)
                result.categories.append(category)
            
            # Process suppliers
            for supplier in sync_data.suppliers:
                existing = await conn.fetchrow('SELECT * FROM suppliers WHERE id = $1', supplier.id)
                if existing:
                    await conn.execute('''
                        UPDATE suppliers SET 
                            name = $1, contact_person = $2, phone = $3,
                            email = $4, address = $5, products = $6,
                            payment_terms = $7
                        WHERE id = $8
                    ''', supplier.name, supplier.contact_person, supplier.phone,
                        supplier.email, supplier.address, supplier.products,
                        supplier.payment_terms, supplier.id)
                else:
                    await conn.execute('''
                        INSERT INTO suppliers (
                            id, name, contact_person, phone, email,
                            address, products, payment_terms
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ''', supplier.id, supplier.name, supplier.contact_person, supplier.phone,
                        supplier.email, supplier.address, supplier.products,
                        supplier.payment_terms)
                result.suppliers.append(supplier)
            
            # Process sales
            for sale in sync_data.sales:
                existing = await conn.fetchrow('SELECT * FROM sales WHERE id = $1', sale.id)
                if existing:
                    await conn.execute('''
                        UPDATE sales SET 
                            date = $1, invoice_number = $2, customer = $3,
                            items = $4::jsonb, payment_method = $5, notes = $6
                        WHERE id = $7
                    ''', make_timezone_naive(sale.date), sale.invoice_number, sale.customer,
                        json.dumps([item.dict() for item in sale.items]), 
                        sale.payment_method, sale.notes, sale.id)
                else:
                    await conn.execute('''
                        INSERT INTO sales (
                            id, date, invoice_number, customer, items,
                            payment_method, notes
                        ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
                    ''', sale.id, make_timezone_naive(sale.date), sale.invoice_number, sale.customer,
                        json.dumps([item.dict() for item in sale.items]), 
                        sale.payment_method, sale.notes)
                result.sales.append(sale)
            
            # Process purchases
            for purchase in sync_data.purchases:
                existing = await conn.fetchrow('SELECT * FROM purchases WHERE id = $1', purchase.id)
                if existing:
                    await conn.execute('''
                        UPDATE purchases SET 
                            date = $1, reference_number = $2, supplier_id = $3,
                            items = $4::jsonb, payment_method = $5, notes = $6
                        WHERE id = $7
                    ''', make_timezone_naive(purchase.date), purchase.reference_number, purchase.supplier_id,
                        json.dumps([item.dict() for item in purchase.items]), 
                        purchase.payment_method, purchase.notes, purchase.id)
                else:
                    await conn.execute('''
                        INSERT INTO purchases (
                            id, date, reference_number, supplier_id, items,
                            payment_method, notes
                        ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
                    ''', purchase.id, make_timezone_naive(purchase.date), purchase.reference_number, purchase.supplier_id,
                        json.dumps([item.dict() for item in purchase.items]), 
                        purchase.payment_method, purchase.notes)
                result.purchases.append(purchase)
            
            # Process adjustments
            for adjustment in sync_data.adjustments:
                existing = await conn.fetchrow('SELECT * FROM adjustments WHERE id = $1', adjustment.id)
                if existing:
                    await conn.execute('''
                        UPDATE adjustments SET 
                            date = $1, product_id = $2, type = $3,
                            quantity = $4, reason = $5, username = $6
                        WHERE id = $7
                    ''', make_timezone_naive(adjustment.date), adjustment.product_id, adjustment.type,
                        adjustment.quantity, adjustment.reason, adjustment.username or "system", adjustment.id)
                else:
                    try:
                        await conn.execute('''
                            INSERT INTO adjustments (
                                id, date, product_id, type, quantity,
                                reason, username
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ''', adjustment.id, make_timezone_naive(adjustment.date), adjustment.product_id, adjustment.type,
                            adjustment.quantity, adjustment.reason, adjustment.username or "system")
                    except asyncpg.UndefinedColumnError:
                        # If the column doesn't exist, add it and try again
                        await conn.execute('ALTER TABLE adjustments ADD COLUMN username TEXT NOT NULL DEFAULT \'system\'')
                        await conn.execute('''
                            INSERT INTO adjustments (
                                id, date, product_id, type, quantity,
                                reason, username
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ''', adjustment.id, make_timezone_naive(adjustment.date), adjustment.product_id, adjustment.type,
                            adjustment.quantity, adjustment.reason, adjustment.username or "system")
                result.adjustments.append(adjustment)
            
            # Process activities - fixed to ensure username is never null
            for activity in sync_data.activities:
                existing = await conn.fetchrow('SELECT * FROM activities WHERE id = $1', activity.id)
                if existing:
                    await conn.execute('''
                        UPDATE activities SET 
                            date = $1, activity = $2, username = $3, details = $4
                        WHERE id = $5
                    ''', make_timezone_naive(activity.date), activity.activity, activity.username or "system", activity.details, activity.id)
                else:
                    try:
                        await conn.execute('''
                            INSERT INTO activities (
                                id, date, activity, username, details
                            ) VALUES ($1, $2, $3, $4, $5)
                        ''', activity.id, make_timezone_naive(activity.date), activity.activity, activity.username or "system", activity.details)
                    except asyncpg.UndefinedColumnError:
                        # If the column doesn't exist, add it and try again
                        await conn.execute('ALTER TABLE activities ADD COLUMN username TEXT NOT NULL DEFAULT \'system\'')
                        await conn.execute('''
                            INSERT INTO activities (
                                id, date, activity, username, details
                            ) VALUES ($1, $2, $3, $4, $5)
                        ''', activity.id, make_timezone_naive(activity.date), activity.activity, activity.username or "system", activity.details)
                result.activities.append(activity)
            
            # Process settings
            if sync_data.settings:
                await conn.execute('''
                    UPDATE settings SET 
                        business_name = $1, currency = $2, tax_rate = $3,
                        low_stock_threshold = $4, invoice_prefix = $5,
                        purchase_prefix = $6, updated_at = CURRENT_TIMESTAMP
                ''', sync_data.settings.business_name, sync_data.settings.currency, sync_data.settings.tax_rate,
                    sync_data.settings.low_stock_threshold, sync_data.settings.invoice_prefix,
                    sync_data.settings.purchase_prefix)
                result.settings = sync_data.settings
            
            # Get all data from server to send back to client
            product_records = await conn.fetch('SELECT * FROM products ORDER BY id')
            result.products = [record_to_product(p) for p in product_records]
            
            category_records = await conn.fetch('SELECT * FROM categories ORDER BY id')
            result.categories = [record_to_category(c) for c in category_records]
            
            supplier_records = await conn.fetch('SELECT * FROM suppliers ORDER BY id')
            result.suppliers = [record_to_supplier(s) for s in supplier_records]
            
            sale_records = await conn.fetch('SELECT * FROM sales ORDER BY id')
            result.sales = [record_to_sale(s) for s in sale_records]
            
            purchase_records = await conn.fetch('SELECT * FROM purchases ORDER BY id')
            result.purchases = [record_to_purchase(p) for p in purchase_records]
            
            adjustment_records = await conn.fetch('SELECT * FROM adjustments ORDER BY id')
            result.adjustments = [record_to_adjustment(a) for a in adjustment_records]
            
            activity_records = await conn.fetch('SELECT * FROM activities ORDER BY id')
            result.activities = [record_to_activity(a) for a in activity_records]
            
            settings_record = await conn.fetchrow('SELECT * FROM settings LIMIT 1')
            if settings_record:
                result.settings = record_to_settings(settings_record)
            
        logger.info(f"Sync completed successfully. Returning {len(result.products)} products, "
                   f"{len(result.categories)} categories, {len(result.sales)} sales")
        
        return result
    
    except Exception as e:
        logger.error(f"Sync error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=str(e)
        )

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}

# Sync status endpoint
@app.get("/sync/status")
async def sync_status():
    return {
        "status": "ready",
        "last_sync_time": datetime.now(timezone.utc).isoformat(),
        "message": "Sync service is operational"
    }
