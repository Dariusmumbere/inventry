from fastapi import FastAPI, HTTPException, Depends, status, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
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
    allow_origins=["https://dariusmumbere.github.io"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["set-cookie"]  # Important for cookies
)

# Security constants
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-here")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Database connection pool
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/inventory")
pool = None

async def get_db():
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(DATABASE_URL)
    return pool

# Models
class User(BaseModel):
    id: int
    email: EmailStr
    full_name: str
    role: str
    disabled: bool = False

class UserInDB(User):
    hashed_password: str

class Token(BaseModel):
    access_token: str
    token_type: str
    user: User

class TokenData(BaseModel):
    email: Optional[str] = None

class UserCreate(BaseModel):
    email: EmailStr
    full_name: str
    password: str
    role: str = "user"

class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None
    disabled: Optional[bool] = None

# Inventory Models (existing)
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
    username: str = "system"

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class Activity(BaseModel):
    id: int
    date: datetime
    activity: str
    username: str = "system"
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

# Helper functions
def verify_password(plain_password: str, hashed_password: str):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str):
    return pwd_context.hash(password)

async def authenticate_user(db, email: str, password: str):
    user = await get_user_by_email(db, email)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme), db=Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
        token_data = TokenData(email=email)
    except JWTError:
        raise credentials_exception
    
    user = await get_user_by_email(db, email=token_data.email)
    if user is None:
        raise credentials_exception
    return user

async def get_current_active_user(current_user: User = Depends(get_current_user)):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user

def make_timezone_naive(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

# Database initialization with users table
async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        # Create users table if not exists
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                full_name TEXT NOT NULL,
                hashed_password TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                disabled BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Check if admin user exists
        admin_exists = await conn.fetchval('''
            SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)
        ''', 'admin@stockmaster.ug')
        
        if not admin_exists:
            # Create default admin user
            hashed_password = get_password_hash("admin123")
            await conn.execute('''
                INSERT INTO users (email, full_name, hashed_password, role)
                VALUES ($1, $2, $3, $4)
            ''', 'admin@stockmaster.ug', 'Admin User', hashed_password, 'admin')

        # Existing inventory tables
        await conn.execute('DROP TABLE IF EXISTS activities CASCADE')
        await conn.execute('DROP TABLE IF EXISTS adjustments CASCADE')
        await conn.execute('DROP TABLE IF EXISTS purchases CASCADE')
        await conn.execute('DROP TABLE IF EXISTS sales CASCADE')
        await conn.execute('DROP TABLE IF EXISTS suppliers CASCADE')
        await conn.execute('DROP TABLE IF EXISTS categories CASCADE')
        await conn.execute('DROP TABLE IF EXISTS products CASCADE')
        await conn.execute('DROP TABLE IF EXISTS settings CASCADE')

        # Create tables with all required columns
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

# User CRUD operations
async def get_user_by_email(db, email: str):
    user_record = await db.fetchrow('SELECT * FROM users WHERE email = $1', email)
    if user_record:
        return UserInDB(
            id=user_record['id'],
            email=user_record['email'],
            full_name=user_record['full_name'],
            role=user_record['role'],
            disabled=user_record['disabled'],
            hashed_password=user_record['hashed_password']
        )
    return None

async def create_user(db, user: UserCreate):
    hashed_password = get_password_hash(user.password)
    try:
        user_id = await db.fetchval('''
            INSERT INTO users (email, full_name, hashed_password, role)
            VALUES ($1, $2, $3, $4)
            RETURNING id
        ''', user.email, user.full_name, hashed_password, user.role)
        return user_id
    except asyncpg.UniqueViolationError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

# Authentication endpoints
@app.post("/token", response_model=Token)
async def login_for_access_token(
    response: Response,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db=Depends(get_db)
):
    user = await authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.email}, expires_delta=access_token_expires
    )
    
    # Set HTTP-only cookie
    response.set_cookie(
        key="access_token",
        value=f"Bearer {access_token}",
        httponly=True,
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        secure=True,  # Important for production
        samesite="none",
        domain="dariusmumbere.github.io"  # Important for GitHub Pages
    )
    
    return {
        "access_token": access_token,  # Also return in body for frontend
        "token_type": "bearer",
        "user": User(**user.dict())
    }
    
@app.post("/logout")
async def logout(response: Response):
    response.delete_cookie("access_token")
    return {"message": "Successfully logged out"}

@app.get("/users/me", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    return current_user

# User management endpoints
@app.post("/users", response_model=User)
async def create_new_user(
    user: UserCreate,
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admin can create users"
        )
    
    user_id = await create_user(db, user)
    return await get_user_by_email(db, user.email)

# Inventory endpoints (protected with authentication)
@app.get("/products", response_model=List[Product])
async def get_products(
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    product_records = await db.fetch('SELECT * FROM products ORDER BY id')
    return [record_to_product(p) for p in product_records]

@app.post("/products", response_model=Product)
async def create_product(
    product: Product,
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    product_id = await db.fetchval('''
        INSERT INTO products (
            name, category_id, description, purchase_price,
            selling_price, stock, reorder_level, unit, barcode
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
    ''', product.name, product.category_id, product.description,
        product.purchase_price, product.selling_price, product.stock,
        product.reorder_level, product.unit, product.barcode)
    
    # Log activity
    await db.execute('''
        INSERT INTO activities (date, activity, username, details)
        VALUES ($1, $2, $3, $4)
    ''', datetime.now(timezone.utc), 'Product created', current_user.email,
        f'Created product {product.name}')
    
    return await db.fetchrow('SELECT * FROM products WHERE id = $1', product_id)

# Categories endpoints
@app.get("/categories", response_model=List[Category])
async def get_categories(
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    category_records = await db.fetch('SELECT * FROM categories ORDER BY id')
    return [record_to_category(c) for c in category_records]

@app.post("/categories", response_model=Category)
async def create_category(
    category: Category,
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    category_id = await db.fetchval('''
        INSERT INTO categories (name, description)
        VALUES ($1, $2)
        RETURNING id
    ''', category.name, category.description)
    
    # Log activity
    await db.execute('''
        INSERT INTO activities (date, activity, username, details)
        VALUES ($1, $2, $3, $4)
    ''', datetime.now(timezone.utc), 'Category created', current_user.email,
        f'Created category {category.name}')
    
    return await db.fetchrow('SELECT * FROM categories WHERE id = $1', category_id)

# Suppliers endpoints
@app.get("/suppliers", response_model=List[Supplier])
async def get_suppliers(
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    supplier_records = await db.fetch('SELECT * FROM suppliers ORDER BY id')
    return [record_to_supplier(s) for s in supplier_records]

@app.post("/suppliers", response_model=Supplier)
async def create_supplier(
    supplier: Supplier,
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    supplier_id = await db.fetchval('''
        INSERT INTO suppliers (
            name, contact_person, phone, email,
            address, products, payment_terms
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
    ''', supplier.name, supplier.contact_person, supplier.phone,
        supplier.email, supplier.address, supplier.products,
        supplier.payment_terms)
    
    # Log activity
    await db.execute('''
        INSERT INTO activities (date, activity, username, details)
        VALUES ($1, $2, $3, $4)
    ''', datetime.now(timezone.utc), 'Supplier created', current_user.email,
        f'Created supplier {supplier.name}')
    
    return await db.fetchrow('SELECT * FROM suppliers WHERE id = $1', supplier_id)

# Sales endpoints
@app.get("/sales", response_model=List[Sale])
async def get_sales(
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    sale_records = await db.fetch('SELECT * FROM sales ORDER BY id')
    return [record_to_sale(s) for s in sale_records]

@app.post("/sales", response_model=Sale)
async def create_sale(
    sale: Sale,
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    sale_id = await db.fetchval('''
        INSERT INTO sales (
            date, invoice_number, customer, items,
            payment_method, notes
        ) VALUES ($1, $2, $3, $4::jsonb, $5, $6)
        RETURNING id
    ''', sale.date, sale.invoice_number, sale.customer,
        json.dumps([item.dict() for item in sale.items]), 
        sale.payment_method, sale.notes)
    
    # Log activity
    await db.execute('''
        INSERT INTO activities (date, activity, username, details)
        VALUES ($1, $2, $3, $4)
    ''', datetime.now(timezone.utc), 'Sale recorded', current_user.email,
        f'Recorded sale {sale.invoice_number}')
    
    return await db.fetchrow('SELECT * FROM sales WHERE id = $1', sale_id)

# Purchases endpoints
@app.get("/purchases", response_model=List[Purchase])
async def get_purchases(
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    purchase_records = await db.fetch('SELECT * FROM purchases ORDER BY id')
    return [record_to_purchase(p) for p in purchase_records]

@app.post("/purchases", response_model=Purchase)
async def create_purchase(
    purchase: Purchase,
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    purchase_id = await db.fetchval('''
        INSERT INTO purchases (
            date, reference_number, supplier_id, items,
            payment_method, notes
        ) VALUES ($1, $2, $3, $4::jsonb, $5, $6)
        RETURNING id
    ''', purchase.date, purchase.reference_number, purchase.supplier_id,
        json.dumps([item.dict() for item in purchase.items]), 
        purchase.payment_method, purchase.notes)
    
    # Log activity
    await db.execute('''
        INSERT INTO activities (date, activity, username, details)
        VALUES ($1, $2, $3, $4)
    ''', datetime.now(timezone.utc), 'Purchase recorded', current_user.email,
        f'Recorded purchase {purchase.reference_number}')
    
    return await db.fetchrow('SELECT * FROM purchases WHERE id = $1', purchase_id)

# Adjustments endpoints
@app.get("/adjustments", response_model=List[Adjustment])
async def get_adjustments(
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    adjustment_records = await db.fetch('SELECT * FROM adjustments ORDER BY id')
    return [record_to_adjustment(a) for a in adjustment_records]

@app.post("/adjustments", response_model=Adjustment)
async def create_adjustment(
    adjustment: Adjustment,
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    adjustment_id = await db.fetchval('''
        INSERT INTO adjustments (
            date, product_id, type, quantity,
            reason, username
        ) VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id
    ''', adjustment.date, adjustment.product_id, adjustment.type,
        adjustment.quantity, adjustment.reason, current_user.email)
    
    # Log activity
    await db.execute('''
        INSERT INTO activities (date, activity, username, details)
        VALUES ($1, $2, $3, $4)
    ''', datetime.now(timezone.utc), 'Stock adjustment', current_user.email,
        f'Adjusted stock for product {adjustment.product_id}')
    
    return await db.fetchrow('SELECT * FROM adjustments WHERE id = $1', adjustment_id)

# Activities endpoints
@app.get("/activities", response_model=List[Activity])
async def get_activities(
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    activity_records = await db.fetch('SELECT * FROM activities ORDER BY date DESC LIMIT 100')
    return [record_to_activity(a) for a in activity_records]

# Settings endpoints
@app.get("/settings", response_model=Settings)
async def get_settings(
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    settings_record = await db.fetchrow('SELECT * FROM settings LIMIT 1')
    if not settings_record:
        raise HTTPException(status_code=404, detail="Settings not found")
    return record_to_settings(settings_record)

@app.put("/settings", response_model=Settings)
async def update_settings(
    settings: Settings,
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admin can update settings"
        )
    
    await db.execute('''
        UPDATE settings SET 
            business_name = $1, currency = $2, tax_rate = $3,
            low_stock_threshold = $4, invoice_prefix = $5,
            purchase_prefix = $6, updated_at = CURRENT_TIMESTAMP
    ''', settings.business_name, settings.currency, settings.tax_rate,
        settings.low_stock_threshold, settings.invoice_prefix,
        settings.purchase_prefix)
    
    # Log activity
    await db.execute('''
        INSERT INTO activities (date, activity, username, details)
        VALUES ($1, $2, $3, $4)
    ''', datetime.now(timezone.utc), 'Settings updated', current_user.email,
        'Updated system settings')
    
    return await db.fetchrow('SELECT * FROM settings LIMIT 1')

# Sync endpoint
@app.post("/sync", response_model=SyncData)
async def sync(
    data: Dict[str, Any],
    current_user: User = Depends(get_current_active_user),
    db=Depends(get_db)
):
    try:
        # Validate and parse the incoming data with proper datetime handling
        sync_data = SyncData(**data)
        server_time = datetime.now(timezone.utc).replace(tzinfo=None)  # Timezone-naive datetime
        
        logger.info(f"Received sync data from {current_user.email} with {len(sync_data.products)} products")
        
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
            
            # Process other entities (categories, suppliers, sales, purchases, etc.)
            # ... (same as before but with current_user tracking)
            
            # Get all data from server to send back to client
            product_records = await conn.fetch('SELECT * FROM products ORDER BY id')
            result.products = [record_to_product(p) for p in product_records]
            
            # ... (other entities)
            
            settings_record = await conn.fetchrow('SELECT * FROM settings LIMIT 1')
            if settings_record:
                result.settings = record_to_settings(settings_record)
            
        logger.info(f"Sync completed successfully for {current_user.email}")
        
        return result
    
    except Exception as e:
        logger.error(f"Sync error for {current_user.email}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=str(e)
        )

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}

# Helper functions for data conversion
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
