from loguru import logger
def product_config():
    product_config ={
        "watches" : {
            "Apple Watch": {
                "versions": ["Series 8", "Series 9", "Series 10"],
                "suffixes": ["", " Ultra", " SE"]
            },
            "Galaxy Watch": {
                "versions": [5, 6, 7],
                "suffixes": ["", " Classic", " Ultra"]
            },
        },
        "tablets" :{
            "iPad": {
                "versions": ["", " Mini", " Air", " Pro"],
                "suffixes": [" 10th Gen", " 11th Gen", " M2", " M4"]
            },
            "Galaxy Tab": {
                "versions": ["S8", "S9", "S10"],
                "suffixes": ["", " FE", " Ultra", " Plus"]
            },
        },
        "laptops":{
            "MacBook": {
                "versions": ["Air", "Pro"],
                "suffixes": [" M2", " M3", " M4"]
            },
            "Dell Inspiron": {
                "versions": [14, 15, 16],
                "suffixes": ["", " Plus", " 2-in-1"]
            },
            "Dell XPS": {
                "versions": [13, 14, 15, 16],
                "suffixes": ["", " Plus"]
            },
            "HP Pavilion": {
                "versions": [14, 15, 16],
                "suffixes": ["", " Plus"]
            },
            "HP Spectre": {
                "versions": [13, 14, 16],
                "suffixes": ["", " x360"]
            },
            "Asus VivoBook": {
                "versions": [14, 15, 16],
                "suffixes": ["", " Pro", " OLED"]
            },
        },
        "phones": {
            "iPhone" :{"versions":range(11,18), "suffixes":[""," Pro"," Pro Max"]},
            "Galaxy S": {"versions": range(20, 26), "suffixes": ["", " Ultra"]},
            "Pixel" :{"versions":range(7,10),"suffixes":[""," Pro"," a"]},
            "OnePlus":{"versions":range(11,14),"suffixes":[""," Pro"," R"]}
        }
    }
    return product_config

def generate_products(**kwargs):
    try:
        products = []
        for product_type, specs in kwargs.items():
            for ver in specs["versions"]:
                version = product_type + " " + str(ver)
                for suf in specs["suffixes"]:
                    products.append(version + suf)
        return products
    except Exception as e:
        logger.error(f"Error generating product data: {e}")
        raise e
