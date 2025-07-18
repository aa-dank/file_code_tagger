import sys
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Import the SQLAlchemy models
from db_models import FilingTag, Base

def get_db_engine():
    """Create and return a SQLAlchemy engine for the project database."""
    conn_string = (
        f"postgresql+psycopg://{os.getenv('PROJECT_DB_USERNAME')}:{os.getenv('PROJECT_DB_PASSWORD')}"
        f"@{os.getenv('PROJECT_DB_HOST')}:{os.getenv('PROJECT_DB_PORT')}/{os.getenv('PROJECT_DB_NAME')}"
    )
    return create_engine(conn_string)

def parse_filing_codes(file_path):
    """Parse the filing codes file and return a list of (code, description) tuples."""
    codes = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('//'):  # Skip empty lines and comments
                continue
            
            # Split by first hyphen with surrounding spaces
            parts = line.split(' - ', 1)
            if len(parts) == 2:
                code = parts[0].strip()
                description = parts[1].strip()
                codes.append((code, description))
    
    return codes

def determine_parent_label(code):
    """
    Determine parent label for a given code based on standard filing code hierarchy.
    
    Rules:
    - Single letter codes (A, B, C) have no parent
    - Codes like A1, B2 have the letter as parent
    - Codes like B8.1 have the code before the dot (B8) as parent
    """
    # Single letter codes (A, B, C, ...) have no parent
    if len(code) == 1 and code.isalpha():
        return None
    
    # For codes with a dot (like B8.1), parent is before the dot
    if '.' in code:
        return code.split('.')[0]
    
    # For codes like A1, B12, parent is the letter part
    for i, char in enumerate(code):
        if i > 0 and char.isdigit():
            return code[:i]
    
    # Default: no parent if we can't determine
    return None

def import_filing_codes(file_path, session):
    """Import filing codes from file to database if they don't exist."""
    codes = parse_filing_codes(file_path)
    
    # Get existing tags as a dictionary for quick lookup
    existing_tags = {tag.label: tag for tag in session.query(FilingTag).all()}
    
    # Process each code
    new_count = 0
    updated_count = 0
    
    for code, description in codes:
        parent_label = determine_parent_label(code)
        
        if code in existing_tags:
            # Check if update needed
            tag = existing_tags[code]
            update_needed = (
                (tag.description != description) or
                ((tag.parent_label is None and parent_label is not None) or
                 (tag.parent_label is not None and tag.parent_label != parent_label))
            )
            
            if update_needed:
                # Update existing tag
                tag.description = description
                tag.parent_label = parent_label
                updated_count += 1
                print(f"Updated tag: {code} - {description} (parent: {parent_label})")
        else:
            # Create new tag
            new_tag = FilingTag(
                label=code,
                parent_label=parent_label,
                description=description,
                importance_rank=1,
                confidence_floor=0.60
            )
            session.add(new_tag)
            new_count += 1
            print(f"Added new tag: {code} - {description} (parent: {parent_label})")
    
    # Commit all changes
    session.commit()
    
    # Print summary
    if new_count > 0:
        print(f"Added {new_count} new filing tags")
    if updated_count > 0:
        print(f"Updated {updated_count} existing tags")
    if new_count == 0 and updated_count == 0:
        print("No changes needed - all tags already exist with correct information")

def main():
    """Main function to run the import process."""
    # Get the path to the filing codes file
    script_dir = Path(__file__).parent
    file_path = script_dir / "dev" / "new_filing_codes.txt"
    
    if not file_path.exists():
        print(f"Error: File not found at {file_path}")
        return 1
    
    print(f"Importing filing codes from {file_path}")
    print("=" * 60)
    
    try:
        # Setup database connection
        engine = get_db_engine()
        Session = sessionmaker(bind=engine)
        
        with Session() as session:
            # Import filing codes
            import_filing_codes(file_path, session)
        return 0
    except Exception as e:
        #print error and stack trace
        import traceback
        traceback.print_exc()
        print(f"Error importing filing codes: {e}")
        return 1

        print(f"Error importing filing codes: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())