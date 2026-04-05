"""
Database table initialization for Capital Changes module
"""

import logging

from sqlalchemy import text

from src.master_data.database import engine
from src.master_data.investors_constants import INVESTORS_TABLE

logger = logging.getLogger("capital_database_ensure")


def ensure_capital_types_table(conn):
    """Create table for storing Capital Types (predefined + custom)"""
    logger.info("🔍 Starting capital_types table creation/verification...")
    
    try:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS capital_types (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                is_system BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        logger.info("✅ capital_types table structure created/verified")
        
        predefined_types = [
            ("Ordinary Shares", True),
            ("Tracer Shares", True),
            ("Founder Shares", True),
            ("GP Shares", True),
        ]
        
        for type_name, is_system in predefined_types:
            conn.execute(text("""
                INSERT INTO capital_types (name, is_system)
                SELECT :name, :is_system
                WHERE NOT EXISTS (
                    SELECT 1 FROM capital_types WHERE name = :name
                )
            """), {"name": type_name, "is_system": is_system})
        
        try:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_types_id 
                ON capital_types(id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_types_name 
                ON capital_types(name)
            """))
            logger.info("✅ capital_types indexes created/verified")
        except Exception as e:
            logger.warning(f"⚠️ Could not create capital_types indexes: {e}")
        
        logger.info("✅ capital_types table ensured successfully")
        
    except Exception as e:
        logger.error(f"❌ Error ensuring capital_types table: {e}")
        raise


def ensure_capital_classes_table(conn):
    """Create table for storing Capital Classes"""
    logger.info("🔍 Starting capital_classes table creation/verification...")
    
    try:
        try:
            result = conn.execute(text("""
                SELECT data_type FROM information_schema.columns 
                WHERE table_name = 'capital_classes' AND column_name = 'company_registration_id'
            """))
            col_type = result.first()
            if col_type and col_type[0] == 'integer':
                logger.info("🔄 Migrating company_registration_id from INTEGER to VARCHAR...")
                conn.execute(text("""
                    ALTER TABLE capital_classes 
                    ALTER COLUMN company_registration_id TYPE VARCHAR(255) USING company_registration_id::text
                """))
                logger.info("✅ Migration completed")
        except Exception as e:
            logger.warning(f"⚠️ Could not check/migrate column type: {e}")
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS capital_classes (
                id SERIAL PRIMARY KEY,
                company_registration_id VARCHAR(255) NOT NULL,
                class_name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(company_registration_id, class_name)
            )
        """))
        logger.info("✅ capital_classes table structure created/verified")
        
        try:
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'entities'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_classes_company'
                        ) THEN
                            ALTER TABLE capital_classes
                            ADD CONSTRAINT fk_capital_classes_company
                            FOREIGN KEY (company_registration_id)
                            REFERENCES entities("Registration ID")
                            ON DELETE CASCADE;
                        END IF;
                    END IF;
                END $$;
            """))
        except Exception as e:
            logger.warning(f"⚠️ Could not add company foreign key: {e}")
        
        try:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_classes_company 
                ON capital_classes(company_registration_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_classes_id 
                ON capital_classes(id)
            """))
            logger.info("✅ capital_classes indexes created/verified")
        except Exception as e:
            logger.warning(f"⚠️ Could not create capital_classes indexes: {e}")
        
        logger.info("✅ capital_classes table ensured successfully")
        
    except Exception as e:
        logger.error(f"❌ Error ensuring capital_classes table: {e}")
        raise


def ensure_capital_sub_classes_table(conn):
    """Create table for storing Capital Sub-Classes"""
    logger.info("🔍 Starting capital_sub_classes table creation/verification...")
    
    try:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS capital_sub_classes (
                id SERIAL PRIMARY KEY,
                class_id INTEGER NOT NULL REFERENCES capital_classes(id) ON DELETE CASCADE,
                sub_class_name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(class_id, sub_class_name)
            )
        """))
        logger.info("✅ capital_sub_classes table structure created/verified")
        
        try:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_sub_classes_class 
                ON capital_sub_classes(class_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_sub_classes_id 
                ON capital_sub_classes(id)
            """))
            logger.info("✅ capital_sub_classes indexes created/verified")
        except Exception as e:
            logger.warning(f"⚠️ Could not create capital_sub_classes indexes: {e}")
        
        logger.info("✅ capital_sub_classes table ensured successfully")
        
    except Exception as e:
        logger.error(f"❌ Error ensuring capital_sub_classes table: {e}")
        raise


def ensure_capital_changes_table(conn):
    """Create table for storing Capital Changes (updated schema)"""
    logger.info("🔍 Starting capital_changes table creation/verification...")
    
    try:
        try:
            table_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = 'capital_changes'
                )
            """)).scalar()
            
            if table_exists:
                try:
                    col_result = conn.execute(text("""
                        SELECT data_type FROM information_schema.columns 
                        WHERE table_name = 'capital_changes' AND column_name = 'company_registration_id'
                    """))
                    col_type = col_result.first()
                    if col_type and col_type[0] == 'integer':
                        logger.info("🔄 Migrating company_registration_id from INTEGER to VARCHAR...")
                        conn.execute(text("""
                            ALTER TABLE capital_changes 
                            ALTER COLUMN company_registration_id TYPE VARCHAR(255) USING company_registration_id::text
                        """))
                        logger.info("✅ company_registration_id type migration completed")
                except Exception as e:
                    logger.warning(f"⚠️ Could not migrate company_registration_id type: {e}")
                
                result = conn.execute(text("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'capital_changes' AND column_name = 'amount'
                """))
                has_old_schema = result.first() is not None
                
                if has_old_schema:
                    logger.info("🔄 Migrating capital_changes table from old schema...")
                    conn.execute(text("""
                        ALTER TABLE capital_changes 
                        ADD COLUMN IF NOT EXISTS investor_id INTEGER,
                        ADD COLUMN IF NOT EXISTS type_id INTEGER,
                        ADD COLUMN IF NOT EXISTS class_id INTEGER,
                        ADD COLUMN IF NOT EXISTS sub_class_id INTEGER,
                        ADD COLUMN IF NOT EXISTS number_of_shares NUMERIC(15, 2),
                        ADD COLUMN IF NOT EXISTS value_per_share NUMERIC(15, 2),
                        ADD COLUMN IF NOT EXISTS currency VARCHAR(3) DEFAULT 'EUR',
                        ADD COLUMN IF NOT EXISTS total NUMERIC(15, 2),
                        ADD COLUMN IF NOT EXISTS event_date DATE DEFAULT CURRENT_DATE
                    """))
                    logger.info("✅ Migration columns added")
                else:
                    currency_exists = conn.execute(text("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name = 'capital_changes' AND column_name = 'currency'
                        )
                    """)).scalar()
                    
                    if not currency_exists:
                        logger.info("🔄 Adding currency column to existing capital_changes table...")
                        conn.execute(text("""
                            ALTER TABLE capital_changes 
                            ADD COLUMN currency VARCHAR(3) NOT NULL DEFAULT 'EUR'
                        """))
                        logger.info("✅ Currency column added")
                    
                    old_value_exists = conn.execute(text("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name = 'capital_changes' AND column_name = 'old_value'
                        )
                    """)).scalar()
                    
                    if not old_value_exists:
                        logger.info("🔄 Adding old_value column to existing capital_changes table...")
                        conn.execute(text("""
                            ALTER TABLE capital_changes 
                            ADD COLUMN old_value NUMERIC(15, 2)
                        """))
                        logger.info("✅ old_value column added")
                    
                    comments_exists = conn.execute(text("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name = 'capital_changes' AND column_name = 'comments'
                        )
                    """)).scalar()
                    
                    if not comments_exists:
                        logger.info("🔄 Adding comments column to existing capital_changes table...")
                        conn.execute(text("""
                            ALTER TABLE capital_changes 
                            ADD COLUMN comments TEXT
                        """))
                        logger.info("✅ comments column added")
        except Exception as e:
            logger.warning(f"⚠️ Could not check/migrate schema: {e}")
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS capital_changes (
                id SERIAL PRIMARY KEY,
                company_registration_id VARCHAR(255) NOT NULL,
                investor_id INTEGER,
                type_id INTEGER NOT NULL,
                class_id INTEGER,
                sub_class_id INTEGER,
                number_of_shares NUMERIC(15, 2) NOT NULL,
                value_per_share NUMERIC(15, 2) NOT NULL,
                currency VARCHAR(3) NOT NULL DEFAULT 'EUR',
                total NUMERIC(15, 2) NOT NULL,
                event_date DATE NOT NULL DEFAULT CURRENT_DATE,
                old_value NUMERIC(15, 2),
                comments TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL
            )
        """))
        logger.info("✅ capital_changes table structure created/verified")
        
        try:
            conn.execute(text("""
                ALTER TABLE capital_changes
                DROP CONSTRAINT IF EXISTS fk_capital_changes_investor
            """))
            
            conn.execute(text(f"""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = '{INVESTORS_TABLE}'
                    ) THEN
                        -- Set investor_id to NULL for rows where investor doesn't exist
                        UPDATE capital_changes
                        SET investor_id = NULL
                        WHERE investor_id IS NOT NULL
                        AND investor_id NOT IN (
                            SELECT id FROM {INVESTORS_TABLE}
                        );
                    END IF;
                END $$;
            """))
            
            conn.execute(text(f"""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = '{INVESTORS_TABLE}'
                    ) THEN
                        ALTER TABLE capital_changes
                        ADD CONSTRAINT fk_capital_changes_investor
                        FOREIGN KEY (investor_id)
                        REFERENCES {INVESTORS_TABLE}(id)
                        ON DELETE SET NULL;
                    END IF;
                END $$;
            """))
            
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'capital_types'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_changes_type'
                        ) THEN
                            ALTER TABLE capital_changes
                            ADD CONSTRAINT fk_capital_changes_type
                            FOREIGN KEY (type_id)
                            REFERENCES capital_types(id);
                        END IF;
                    END IF;
                END $$;
            """))
            
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'capital_classes'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_changes_class'
                        ) THEN
                            ALTER TABLE capital_changes
                            ADD CONSTRAINT fk_capital_changes_class
                            FOREIGN KEY (class_id)
                            REFERENCES capital_classes(id)
                            ON DELETE SET NULL;
                        END IF;
                    END IF;
                END $$;
            """))
            
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'capital_sub_classes'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_changes_sub_class'
                        ) THEN
                            ALTER TABLE capital_changes
                            ADD CONSTRAINT fk_capital_changes_sub_class
                            FOREIGN KEY (sub_class_id)
                            REFERENCES capital_sub_classes(id)
                            ON DELETE SET NULL;
                        END IF;
                    END IF;
                END $$;
            """))
            
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'entities'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_changes_company'
                        ) THEN
                            ALTER TABLE capital_changes
                            ADD CONSTRAINT fk_capital_changes_company
                            FOREIGN KEY (company_registration_id)
                            REFERENCES entities("Registration ID");
                        END IF;
                    END IF;
                END $$;
            """))
        except Exception as e:
            logger.warning(f"⚠️ Could not add foreign keys: {e}")
        
        try:
            currency_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'capital_changes' AND column_name = 'currency'
                )
            """)).scalar()
            
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_changes_company 
                ON capital_changes(company_registration_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_changes_investor 
                ON capital_changes(investor_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_changes_type 
                ON capital_changes(type_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_changes_class 
                ON capital_changes(class_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_changes_sub_class 
                ON capital_changes(sub_class_id)
            """))
            
            if currency_exists:
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_capital_changes_currency 
                    ON capital_changes(currency)
                """))
            else:
                logger.warning("⚠️ Currency column does not exist, skipping currency index creation")
            
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_changes_event_date 
                ON capital_changes(event_date)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_changes_old_value 
                ON capital_changes(old_value)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_changes_comments 
                ON capital_changes USING gin(to_tsvector('english', COALESCE(comments, '')))
            """))
            logger.info("✅ capital_changes indexes created/verified")
        except Exception as e:
            logger.warning(f"⚠️ Could not create capital_changes indexes: {e}")
        
        logger.info("✅ capital_changes table ensured successfully")
        
    except Exception as e:
        logger.error(f"❌ Error ensuring capital_changes table: {e}")
        raise


def ensure_capital_log_table(conn):
    """Create table for storing capital change history (capital log)"""
    logger.info("🔍 Starting capital_log table creation/verification...")
    
    try:
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'capital_log'
            )
        """)).scalar()
        
        if table_exists:
            try:
                result = conn.execute(text("""
                    SELECT data_type FROM information_schema.columns 
                    WHERE table_name = 'capital_log' AND column_name = 'company_registration_id'
                """))
                col_type = result.first()
                if col_type and col_type[0] == 'integer':
                    logger.info("🔄 Migrating company_registration_id from INTEGER to VARCHAR...")
                    conn.execute(text("""
                        ALTER TABLE capital_log 
                        ALTER COLUMN company_registration_id TYPE VARCHAR(255) USING company_registration_id::text
                    """))
                    logger.info("✅ Migration completed")
            except Exception as e:
                logger.warning(f"⚠️ Could not check/migrate column type: {e}")
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS capital_log (
                id SERIAL PRIMARY KEY,
                company_registration_id VARCHAR(255) NOT NULL,
                event_date DATE NOT NULL,
                event_type VARCHAR(20) NOT NULL CHECK (event_type IN ('Increase', 'Decrease')),
                old_capital NUMERIC(15, 2) NOT NULL,
                new_capital NUMERIC(15, 2) NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER REFERENCES users(id) ON DELETE SET NULL
            )
        """))
        logger.info("✅ capital_log table structure created/verified")
        
        try:
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'entities'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_log_company'
                        ) THEN
                            ALTER TABLE capital_log
                            ADD CONSTRAINT fk_capital_log_company
                            FOREIGN KEY (company_registration_id)
                            REFERENCES entities("Registration ID")
                            ON DELETE CASCADE;
                        END IF;
                    END IF;
                END $$;
            """))
        except Exception as e:
            logger.warning(f"⚠️ Could not add company foreign key: {e}")
        
        try:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_log_company 
                ON capital_log(company_registration_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_log_event_date 
                ON capital_log(event_date)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_log_event_type 
                ON capital_log(event_type)
            """))
            logger.info("✅ capital_log indexes created/verified")
        except Exception as e:
            logger.warning(f"⚠️ Could not create capital_log indexes: {e}")
        
        logger.info("✅ capital_log table ensured successfully")
        
    except Exception as e:
        logger.error(f"❌ Error ensuring capital_log table: {e}")
        raise


def ensure_capital_log_changes_table(conn):
    """Create table for tracking detailed changes per investor per event"""
    logger.info("🔍 Starting capital_log_changes table creation/verification...")
    
    try:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS capital_log_changes (
                id SERIAL PRIMARY KEY,
                capital_log_id INTEGER NOT NULL REFERENCES capital_log(id) ON DELETE CASCADE,
                investor_id INTEGER,
                capital_change_id INTEGER REFERENCES capital_changes(id) ON DELETE SET NULL,
                before_number_of_shares NUMERIC(15, 2),
                before_value_per_share NUMERIC(15, 2),
                before_total NUMERIC(15, 2),
                before_class_id INTEGER,
                before_sub_class_id INTEGER,
                before_type_id INTEGER,
                after_number_of_shares NUMERIC(15, 2),
                after_value_per_share NUMERIC(15, 2),
                after_total NUMERIC(15, 2),
                after_class_id INTEGER,
                after_sub_class_id INTEGER,
                after_type_id INTEGER,
                change_type VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        logger.info("✅ capital_log_changes table structure created/verified")
        
        try:
            conn.execute(text(f"""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = '{INVESTORS_TABLE}'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_log_changes_investor'
                        ) THEN
                            ALTER TABLE capital_log_changes
                            ADD CONSTRAINT fk_capital_log_changes_investor
                            FOREIGN KEY (investor_id)
                            REFERENCES {INVESTORS_TABLE}(id)
                            ON DELETE SET NULL;
                        END IF;
                    END IF;
                END $$;
            """))
        except Exception as e:
            logger.warning(f"⚠️ Could not add investor foreign key: {e}")
        
        try:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_log_changes_log 
                ON capital_log_changes(capital_log_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_log_changes_investor 
                ON capital_log_changes(investor_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_log_changes_capital_change 
                ON capital_log_changes(capital_change_id)
            """))
            logger.info("✅ capital_log_changes indexes created/verified")
        except Exception as e:
            logger.warning(f"⚠️ Could not create capital_log_changes indexes: {e}")
        
        logger.info("✅ capital_log_changes table ensured successfully")
        
    except Exception as e:
        logger.error(f"❌ Error ensuring capital_log_changes table: {e}")
        raise


def ensure_capital_audit_log_table(conn):
    """Create table for storing audit logs of capital changes"""
    logger.info("🔍 Starting capital_audit_log table creation/verification...")
    
    try:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS capital_audit_log (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(50) NOT NULL,
                row_id INTEGER NOT NULL,
                column_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                user_id INTEGER NOT NULL,
                username TEXT,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                change_set_id TEXT
            )
        """))
        logger.info("✅ capital_audit_log table structure created/verified")
        
        try:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_audit_table_row 
                ON capital_audit_log(table_name, row_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_audit_user 
                ON capital_audit_log(user_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_audit_timestamp 
                ON capital_audit_log(changed_at)
            """))
            logger.info("✅ capital_audit_log indexes created/verified")
        except Exception as e:
            logger.warning(f"⚠️ Could not create capital_audit_log indexes: {e}")
        
        logger.info("✅ capital_audit_log table ensured successfully")
        
    except Exception as e:
        logger.error(f"❌ Error ensuring capital_audit_log table: {e}")
        raise


def init_capital_database():
    """
    Initialize all Capital Changes database tables
    
    This function is called from the shared database_init.py
    to ensure all capital tables exist.
    """
    logger.info("🚀 Initializing Capital database...")
    
    try:
        with engine.connect() as conn:
            try:
                ensure_capital_types_table(conn)
                conn.commit()
            except Exception as e:
                logger.warning(f"⚠️ Error ensuring capital_types table: {e}")
                conn.rollback()
            
            try:
                ensure_capital_classes_table(conn)
                conn.commit()
            except Exception as e:
                logger.warning(f"⚠️ Error ensuring capital_classes table: {e}")
                conn.rollback()
            
            try:
                ensure_capital_sub_classes_table(conn)
                conn.commit()
            except Exception as e:
                logger.warning(f"⚠️ Error ensuring capital_sub_classes table: {e}")
                conn.rollback()
            
            try:
                ensure_capital_audit_log_table(conn)
                conn.commit()
            except Exception as e:
                logger.warning(f"⚠️ Error ensuring capital_audit_log table: {e}")
                conn.rollback()
            
            try:
                ensure_capital_events_table(conn)
                conn.commit()
            except Exception as e:
                logger.warning(f"⚠️ Error ensuring capital_events table: {e}")
                conn.rollback()
            
            try:
                ensure_capital_allocations_table(conn)
                conn.commit()
            except Exception as e:
                logger.warning(f"⚠️ Error ensuring capital_allocations table: {e}")
                conn.rollback()
            
            logger.info("✅ All Capital tables initialization attempted")
            return True
            
    except Exception as e:
        logger.error(f"❌ Error initializing Capital database: {e}")
        raise


def ensure_capital_events_table(conn):
    """Create new simplified table for storing Capital Events with all details"""
    logger.info("🔍 Starting new capital_events table creation/verification...")
    
    try:
        # Check if table exists with new schema
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'capital_events'
            )
        """)).scalar()
        
        # Only drop and recreate if table doesn't exist or has old schema
        if not table_exists:
            # Drop old tables first (if they exist) - only if creating new table
            try:
                conn.execute(text("DROP TABLE IF EXISTS capital_events_changes CASCADE"))
                logger.info("✅ Dropped capital_events_changes table")
            except Exception as e:
                logger.warning(f"⚠️ Could not drop capital_events_changes: {e}")
            
            try:
                conn.execute(text("DROP TABLE IF EXISTS capital_changes CASCADE"))
                logger.info("✅ Dropped capital_changes table")
            except Exception as e:
                logger.warning(f"⚠️ Could not drop capital_changes: {e}")
            
            # Create new simplified capital_events table
            conn.execute(text("""
                CREATE TABLE capital_events (
                id SERIAL PRIMARY KEY,
                entity_id VARCHAR(255) NOT NULL,
                event_date DATE NOT NULL,
                event_type VARCHAR(20) NOT NULL CHECK (event_type IN ('Increase', 'Decrease')),
                type_id INTEGER NOT NULL,
                class_id INTEGER,
                sub_class_id INTEGER,
                old_value NUMERIC(15, 2),
                new_value NUMERIC(15, 2) NOT NULL,
                denomination VARCHAR(10),
                value_per_share NUMERIC(15, 2) NOT NULL,
                allocation NUMERIC(15, 2) NOT NULL,
                total_allocation NUMERIC(15, 2) NOT NULL,
                comments TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                deleted_at TIMESTAMP NULL,
                is_deleted BOOLEAN DEFAULT FALSE
            )
        """))
            logger.info("✅ New capital_events table structure created")
        else:
            logger.info("✅ capital_events table already exists, checking for soft delete fields")
            # Add soft delete fields if they don't exist
            try:
                conn.execute(text("""
                    DO $$ BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name = 'capital_events' AND column_name = 'deleted_at'
                        ) THEN
                            ALTER TABLE capital_events ADD COLUMN deleted_at TIMESTAMP NULL;
                        END IF;
                    END $$;
                """))
                conn.execute(text("""
                    DO $$ BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name = 'capital_events' AND column_name = 'is_deleted'
                        ) THEN
                            ALTER TABLE capital_events ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE;
                        END IF;
                    END $$;
                """))
                logger.info("✅ Soft delete fields added to capital_events table")
            except Exception as e:
                logger.warning(f"⚠️ Could not add soft delete fields: {e}")
        
        # Add foreign keys
        try:
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'entities'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_events_entity'
                        ) THEN
                            ALTER TABLE capital_events
                            ADD CONSTRAINT fk_capital_events_entity
                            FOREIGN KEY (entity_id)
                            REFERENCES entities("Registration ID")
                            ON DELETE CASCADE;
                        END IF;
                    END IF;
                END $$;
            """))
            
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'capital_types'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_events_type'
                        ) THEN
                            ALTER TABLE capital_events
                            ADD CONSTRAINT fk_capital_events_type
                            FOREIGN KEY (type_id)
                            REFERENCES capital_types(id);
                        END IF;
                    END IF;
                END $$;
            """))
            
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'capital_classes'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_events_class'
                        ) THEN
                            ALTER TABLE capital_events
                            ADD CONSTRAINT fk_capital_events_class
                            FOREIGN KEY (class_id)
                            REFERENCES capital_classes(id)
                            ON DELETE SET NULL;
                        END IF;
                    END IF;
                END $$;
            """))
            
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'capital_sub_classes'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_events_sub_class'
                        ) THEN
                            ALTER TABLE capital_events
                            ADD CONSTRAINT fk_capital_events_sub_class
                            FOREIGN KEY (sub_class_id)
                            REFERENCES capital_sub_classes(id)
                            ON DELETE SET NULL;
                        END IF;
                    END IF;
                END $$;
            """))
        except Exception as e:
            logger.warning(f"⚠️ Could not add foreign keys: {e}")
        
        # Create indexes
        try:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_events_entity 
                ON capital_events(entity_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_events_event_date 
                ON capital_events(event_date)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_events_event_type 
                ON capital_events(event_type)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_events_type 
                ON capital_events(type_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_events_class 
                ON capital_events(class_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_events_sub_class 
                ON capital_events(sub_class_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_events_is_deleted 
                ON capital_events(is_deleted)
            """))
            logger.info("✅ capital_events indexes created/verified")
        except Exception as e:
            logger.warning(f"⚠️ Could not create capital_events indexes: {e}")
        
        logger.info("✅ New capital_events table ensured successfully")
        
    except Exception as e:
        logger.error(f"❌ Error ensuring capital_events table: {e}")
        raise


def ensure_capital_allocations_table(conn):
    """Create table for storing Capital Allocations"""
    logger.info("🔍 Starting capital_allocations table creation/verification...")
    
    try:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS capital_allocations (
                id SERIAL PRIMARY KEY,
                entity_id VARCHAR(255) NOT NULL,
                investor_id INTEGER,
                event_id INTEGER,
                share_type_id INTEGER,
                class_id INTEGER,
                sub_class_id INTEGER,
                number_of_shares NUMERIC(15, 2) NOT NULL,
                value_per_share NUMERIC(15, 2) NOT NULL,
                denomination VARCHAR(10),
                total NUMERIC(15, 2) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL
            )
        """))
        logger.info("✅ capital_allocations table structure created/verified")
        
        try:
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'entities'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_allocations_entity'
                        ) THEN
                            ALTER TABLE capital_allocations
                            ADD CONSTRAINT fk_capital_allocations_entity
                            FOREIGN KEY (entity_id)
                            REFERENCES entities("Registration ID")
                            ON DELETE CASCADE;
                        END IF;
                    END IF;
                END $$;
            """))
            
            conn.execute(text("""
                DO $$ BEGIN
                    -- Drop old constraint if exists
                    ALTER TABLE capital_allocations
                    DROP CONSTRAINT IF EXISTS fk_capital_allocations_event;
                    
                    -- Add correct constraint with CASCADE
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'capital_events'
                    ) THEN
                        ALTER TABLE capital_allocations
                        ADD CONSTRAINT fk_capital_allocations_event
                        FOREIGN KEY (event_id)
                        REFERENCES capital_events(id)
                        ON DELETE CASCADE;
                    END IF;
                END $$;
            """))
            
            conn.execute(text(f"""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = '{INVESTORS_TABLE}'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_allocations_investor'
                        ) THEN
                            ALTER TABLE capital_allocations
                            ADD CONSTRAINT fk_capital_allocations_investor
                            FOREIGN KEY (investor_id)
                            REFERENCES {INVESTORS_TABLE}(id)
                            ON DELETE SET NULL;
                        END IF;
                    END IF;
                END $$;
            """))
            
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'share_types'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_allocations_share_type'
                        ) THEN
                            ALTER TABLE capital_allocations
                            ADD CONSTRAINT fk_capital_allocations_share_type
                            FOREIGN KEY (share_type_id)
                            REFERENCES share_types(id)
                            ON DELETE SET NULL;
                        END IF;
                    END IF;
                END $$;
            """))
            
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'capital_classes'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_allocations_class'
                        ) THEN
                            ALTER TABLE capital_allocations
                            ADD CONSTRAINT fk_capital_allocations_class
                            FOREIGN KEY (class_id)
                            REFERENCES capital_classes(id)
                            ON DELETE SET NULL;
                        END IF;
                    END IF;
                END $$;
            """))
            
            conn.execute(text("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'capital_sub_classes'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'fk_capital_allocations_sub_class'
                        ) THEN
                            ALTER TABLE capital_allocations
                            ADD CONSTRAINT fk_capital_allocations_sub_class
                            FOREIGN KEY (sub_class_id)
                            REFERENCES capital_sub_classes(id)
                            ON DELETE SET NULL;
                        END IF;
                    END IF;
                END $$;
            """))
        except Exception as e:
            logger.warning(f"⚠️ Could not add foreign keys: {e}")
        
        try:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_allocations_entity 
                ON capital_allocations(entity_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_allocations_investor 
                ON capital_allocations(investor_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_allocations_event 
                ON capital_allocations(event_id)
            """))
            logger.info("✅ capital_allocations indexes created/verified")
        except Exception as e:
            logger.warning(f"⚠️ Could not create capital_allocations indexes: {e}")
        
        logger.info("✅ capital_allocations table ensured successfully")
        
    except Exception as e:
        logger.error(f"❌ Error ensuring capital_allocations table: {e}")
        raise


def ensure_capital_events_changes_table(conn):
    """Create junction table linking capital_events to capital_changes"""
    logger.info("🔍 Starting capital_events_changes table creation/verification...")
    
    try:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS capital_events_changes (
                id SERIAL PRIMARY KEY,
                event_id INTEGER NOT NULL REFERENCES capital_events(id) ON DELETE CASCADE,
                capital_change_id INTEGER NOT NULL REFERENCES capital_changes(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(event_id, capital_change_id)
            )
        """))
        logger.info("✅ capital_events_changes table structure created/verified")
        
        try:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_events_changes_event 
                ON capital_events_changes(event_id)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_capital_events_changes_change 
                ON capital_events_changes(capital_change_id)
            """))
            logger.info("✅ capital_events_changes indexes created/verified")
        except Exception as e:
            logger.warning(f"⚠️ Could not create capital_events_changes indexes: {e}")
        
        logger.info("✅ capital_events_changes table ensured successfully")
        
    except Exception as e:
        logger.error(f"❌ Error ensuring capital_events_changes table: {e}")
        raise