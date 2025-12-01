from driver import get_driver
from queries import SETUP


def main():
    driver = get_driver()
    if not driver:
        return 
    
    try:
        with driver.session() as session:
            for query in constraints:
                session.execute_write(lambda tx: tx.run(SETUP))

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        driver.close()


if __name__ == "__main__":
    main()