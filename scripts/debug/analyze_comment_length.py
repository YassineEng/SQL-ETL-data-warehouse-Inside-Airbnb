
import sys
import os
import pyodbc

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import Config
from config.database_config import DatabaseConfig
from utils.logger import get_logger

logger = get_logger(__name__)

def analyze_comment_length(num_comments: int = 20):
    """
    Fetches a few random comments and analyzes their length.
    """
    conn = None
    try:
        config = Config()
        db_config = DatabaseConfig(config)
        
        print("Connecting to the database...")
        conn = db_config.create_connection()
        cursor = conn.cursor()
        print("Database connection successful.")

        query = f"""
        SELECT TOP {num_comments} comments
        FROM fact_reviews
        WHERE comments IS NOT NULL AND LEN(comments) > 0
        ORDER BY NEWID(); -- ORDER BY NEWID() for random selection in SQL Server
        """
        
        cursor.execute(query)
        results = cursor.fetchall()

        if results:
            print(f"\nAnalyzing {len(results)} random comments:")
            for i, row in enumerate(results):
                comment = row[0]
                comment_length = len(comment)
                snippet = comment[:200] # Display first 200 chars for analysis
                print(f"-- Comment {i+1} --")
                print(f"Length: {comment_length}")
                print(f"Snippet (first 200 chars): {snippet}")
                print("-------------------")
        else:
            print("No comments found for analysis.")

    except pyodbc.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    analyze_comment_length(num_comments=20)
