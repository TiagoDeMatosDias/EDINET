import logging
import src.orchestrator as o
from src.logger import setup_logging

if __name__ == '__main__':
    # Set up logging
    logger, log_path = setup_logging()
    logger.info("=" * 80)
    logger.info("APPLICATION START")
    logger.info("=" * 80)
    
    try:
        o.run()
        logger.info("=" * 80)
        logger.info("APPLICATION COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
    except Exception as e:
        logger.error(f"Application failed with error: {e}", exc_info=True)
        raise