# **Projeto: Processamento de BigData com PySpark**

## **Stack**
Python (PySpark, pandas, matplot, seaborn), Jupyter Notebook.

## **Sobre**
Projeto de operações em BigData(Fundamentos Básicos, Limpeza e Transformação, Geração de Insights, Gráficos, etc) utilizando PySpark para o processamento dos dados / DataFrames pandas e Matplotlib / Seaborn para construção de gráficos.

## **Sobre os Dados**
Os dados foram retirados de um sample de Amazon products dataset (BigData), ou seja, são apenas 1000 linhas para exemplificar as operações de BigData realizadas no Jupyter Notebook. O DataSet completo é pago e pode ser encontrado na plataforma: https://brightdata.com.

## **Jupyter Notebook:**
Para acessar detalhadamente as análises e processos Jupyter Notebook, clique no link: https://github.com/caioypaulino/Projeto-BigData_Analytics_PySpark/blob/main/big_data_analytics.ipynb

## **Exemplo Incompleto Jupyter:**
Abaixo segue apenas um exemplo do código Python:
_________________________________________________

<div class="cell markdown" collapsed="false">

# BIG DATA ANALYTICS COM <font color='green'>PYSPARK</font>

</div>

<div class="cell code"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:08.290736100Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:08.194023Z&quot;}"
collapsed="false">

``` python
# importando bibliotecas
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker

from pyspark.sql import (SparkSession)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.functions import col, sum, countDistinct, count, isnan, when, regexp_replace, split, month, year, size, element_at, struct, trim, avg, expr, lit
from pyspark.sql import functions as F
from pyspark.sql.window import Window
```

</div>

<div class="cell markdown" collapsed="false">

## Operações Básicas com PySpark

</div>

<div class="cell code" execution_count="2"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:08.357214300Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:08.195024300Z&quot;}"
collapsed="false">

``` python
# Criando SparkSession
spark = (SparkSession.builder
         .appName("AmazonElectronics")
         .getOrCreate())
```

</div>

<div class="cell code" execution_count="3"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:08.410706400Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:08.380377800Z&quot;}"
collapsed="true">

``` python
# Definindo Schema
schema = StructType([
    StructField("timestamp", DateType(), True),
    StructField("asin", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("buybox_seller", StringType(), True),
    StructField("final_price", StringType(), True),
    StructField("number_of_sellers", IntegerType(), True),
    StructField("root_bs_rank", IntegerType(), True),
    StructField("reviews_count", IntegerType(), True),
    StructField("currency", StringType(), True),
    StructField("image_url", StringType(), True),
    StructField("images_count", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("video_count", IntegerType(), True),
    StructField("categories", StringType(), True),
    StructField("item_weight", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("seller_id", StringType(), True),
    StructField("availability", StringType(), True),
    StructField("product_dimensions", StringType(), True),
    StructField("discount", StringType(), True),
    StructField("initial_price", StringType(), True),
    StructField("description", StringType(), True),
    StructField("image", StringType(), True),
    StructField("answered_questions", IntegerType(), True),
    StructField("date_first_available", StringType(), True),
    StructField("model_number", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("department", StringType(), True),
    StructField("plus_content", StringType(), True),
    StructField("upc", StringType(), True),
    StructField("video", StringType(), True),
])
```

</div>

<div class="cell code" execution_count="4"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:10.915481900Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:08.398579600Z&quot;}"
collapsed="false">

``` python
# Definindo o caminho do arquivo .csv
file_path = '.\\amazon_electronics.csv'

# Start timer
start_time = time.time()

# (Read) Lendo arquivo .csv e atribuindo a um DataFrame
df = spark.read.csv(file_path, schema=schema, header=True, quote='"',escape='"')

# Fim timer
end_time = time.time()

# Calculando o tempo de execução
exec_time = (end_time - start_time)

# Get número de linhas DataFrame
num_rows = df.count()

# Get número de colunas DataFrame
num_columns = len(df.columns)

# Exibindo Shape
print("Shape:{} linhas e {} colunas".format(num_rows, num_columns))

# Exibindo o tempo de execução
print("Tempo de Execução usando PySpark:",exec_time,"segundos")
```

<div class="output stream stdout">

    Shape:1000 linhas e 32 colunas
    Tempo de Execução usando PySpark: 1.6812539100646973 segundos

</div>

</div>

<div class="cell code" execution_count="5"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:10.935722700Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:10.920480800Z&quot;}"
collapsed="false">

``` python
# Exibindo o máximo de 34 colunas para que possamos percorrer todo o nosso Dataset horizontalmente 
pd.options.display.max_columns = 32
```

</div>

<div class="cell code" execution_count="6"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:11.268477800Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:10.936723500Z&quot;}"
collapsed="false">

``` python
# Checando as primeiras 6 linhas do DataFrame
df.limit(6).toPandas()
```

<div class="output execute_result" execution_count="6">

        timestamp        asin                     brand  buybox_seller  \
    0  2023-08-03  B07RGHBLGC                  SHNITPWR      SNT-POWER   
    1  2023-08-15  B09871LZYT                    FINTIE         Fintie   
    2  2023-08-16  B09W9BXT9Z                   Nixplay     Amazon.com   
    3  2022-10-13  B093F837T9  Sound Storm Laboratories   Speece, Inc.   
    4  2024-02-04  B0C2YQ9BJ1                  Redragon  Redragon Shop   
    5  2024-01-21  B08D3Y5PFZ                       AOC     Amazon.com   

      final_price  number_of_sellers  root_bs_rank  reviews_count currency  \
    0       13.99                  1           NaN            234      USD   
    1       15.99                  2       12194.0           1576      USD   
    2      219.99                  3         366.0           1998      USD   
    3     $335.99                  2       45741.0            324        $   
    4       39.99                  1          91.0           8219      USD   
    5      129.99                 18        2417.0          14803      USD   

                                               image_url  images_count  \
    0  https://m.media-amazon.com/images/I/61qDGTATaL...             7   
    1  https://m.media-amazon.com/images/I/81Adh+cGYc...             1   
    2  https://m.media-amazon.com/images/I/71iSS-r+KJ...             1   
    3  https://m.media-amazon.com/images/I/71DXeNXt84...             5   
    4  https://m.media-amazon.com/images/I/71chbo4DCq...             1   
    5  https://m.media-amazon.com/images/I/71aXlu6n1q...             1   

                                                   title  \
    0  SHNITPWR 12V 6A AC DC Power Supply Adapter Con...   
    1  Fintie Silicone Case for All-New Fire HD 10 an...   
    2  Nixplay 10.1 inch Touch Screen Digital Picture...   
    3  Sound Storm Laboratories SDML10ACP Single Din ...   
    4  Redragon GS520 RGB Desktop Speakers, 2.0 Chann...   
    5  AOC C24G1A 24" Curved Frameless Gaming Monitor...   

                                                     url  video_count  \
    0  https://www.amazon.com/SHNITPWR-Converter-100V...            0   
    1  https://www.amazon.com/Fintie-Silicone-All-New...            0   
    2  https://www.amazon.com/Nixplay-Digital-W10K-Po...            0   
    3  https://www.amazon.com/dp/B093F837T9?language=...            1   
    4  https://www.amazon.com/Redragon-GS520-Speakers...            0   
    5  https://www.amazon.com/AOC-C24G1A-Frameless-19...            0   

                                              categories   item_weight  rating  \
    0          Electronics,Power Accessories,AC Adapters     400 Grams     4.4   
    1  Electronics,Computers & Accessories,Tablet Acc...    7.4 ounces     4.7   
    2  Electronics,Camera & Photo,Lighting & Studio,P...   1.63 pounds     4.6   
    3  Electronics,Car & Vehicle Electronics,Car Elec...   5.45 pounds     4.1   
    4  Electronics,Computers & Accessories,Computer A...  0.071 ounces     4.4   
    5       Electronics,Computers & Accessories,Monitors   9.92 pounds     4.7   

            seller_id                       availability  \
    0  A2QEP0IMOO32F1                           In Stock   
    1  A3A3E6QGUGPEMU                           In Stock   
    2   ATVPDKIKX0DER  Only 5 left in stock - order soon   
    3   ABO907060G8YG                          In Stock.   
    4  A2FK9EP27A6ZE6                           In Stock   
    5   ATVPDKIKX0DER                           In Stock   

                product_dimensions discount initial_price  \
    0             8 x 5 x 2 inches       42         23.99   
    1    5.12 x 1.77 x 0.67 inches     -52%         32.99   
    2   10.55 x 4.65 x 0.99 inches     -25%        219.99   
    3    4.94 x 7.01 x 1.97 inches     null          null   
    4    7.36 x 4.33 x 6.69 inches      -5%         41.99   
    5  9.64 x 21.14 x 20.19 inches     -13%        149.99   

                                             description  \
    0  About this item Input: AC 100 - 240V, 50 / 60H...   
    1  Specifically designed for All-New Amazon Fire ...   
    2  Nixplay Digital Picture Frame with Touch Scree...   
    3  Sound Storm Laboratories SDML10ACP Apple CarPl...   
    4  Plug & Play, Broad Compatibility USB powered w...   
    5  AOC C24G1A 24-inch Class Curved Gaming Monitor...   

                                                   image  answered_questions  \
    0  https://m.media-amazon.com/images/I/61qDGTATaL...                   0   
    1  https://m.media-amazon.com/images/I/81Adh+cGYc...                   0   
    2  https://m.media-amazon.com/images/I/71iSS-r+KJ...                   0   
    3  https://m.media-amazon.com/images/I/71DXeNXt84...                 146   
    4  https://m.media-amazon.com/images/I/71chbo4DCq...                   0   
    5  https://m.media-amazon.com/images/I/71aXlu6n1q...                   0   

      date_first_available           model_number              manufacturer  \
    0         May 10, 2019               SNT-1206                  SHNITPWR   
    1       March 15, 2021                EKAR025                    FINTIE   
    2        April 6, 2022  W10K - Polished Steel                   Nixplay   
    3     January 21, 2021              SDML10ACP  Sound Storm Laboratories   
    4      August 24, 2020                  GS520                  Redragon   
    5   September 15, 2020                 C24G1A                       AOC   

        department plus_content   upc  video  
    0         null         true  null  false  
    1         null         true  null  false  
    2         null        false  null  false  
    3         null         true  null   true  
    4  Electronics         true  null  false  
    5  Electronics         true  null  false  

</div>

</div>

<div class="cell code" execution_count="7"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:11.401667200Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:11.273564500Z&quot;}"
collapsed="false">

``` python
df.limit(6).collect()
```

<div class="output execute_result" execution_count="7">

    [Row(timestamp=datetime.date(2023, 8, 3), asin='B07RGHBLGC', brand='SHNITPWR', buybox_seller='SNT-POWER', final_price='13.99', number_of_sellers=1, root_bs_rank=None, reviews_count=234, currency='USD', image_url='https://m.media-amazon.com/images/I/61qDGTATaLL.__AC_SX300_SY300_QL70_FMwebp_.jpg', images_count=7, title='SHNITPWR 12V 6A AC DC Power Supply Adapter Converter 100V~240V AC to DC 12 Volt 6 Amp 72W LED Driver Transformer with 5.5x2.5mm Plug for 5050 3528 LED Strip 3D Printer CCTV Security System LCD Monitor', url='https://www.amazon.com/SHNITPWR-Converter-100V-240V-Transformer-5-5x2-5mm/dp/B07RGHBLGC', video_count=0, categories='Electronics,Power Accessories,AC Adapters', item_weight='400 Grams', rating=4.4, seller_id='A2QEP0IMOO32F1', availability='In Stock', product_dimensions='8 x 5 x 2 inches', discount='42', initial_price='23.99', description="About this item Input: AC 100 - 240V, 50 / 60Hz ; Output: DC 12V, Max 6A, 72W ; It can supply with all amperage less than 6A. ---such as 500mA, 1A, 2.5A, 3A, 5A, 6A. If your device draws 4A then 4A will be supplied. But if draws more than 6A, only 6A will be supplied and power supply will be damaged soon. NEVER OVERLOAD！ Please pay attention to the size and polarity of the power plug.The polarity of this adapter is Central Positive(+) and Outer Negative(-).And it can Only be used for devices with sockets of 5.5 x 2.1mm! If you are not sure whether the adapter matches your device, feel free to contact us by email. Certified by FCC CE ROHS. We focus on producing high quality power adapters. No noise, low temperature operation, no spontaneous combustion, no explosion, no fire hazard, stable output. Automatic overload cut-off, over voltage cut-off, automatic thermal cut-off, short circuit protection. It is perfect for 5050 3528 12V LED Strip Light, Wireless Router, ADSL Cats, HUB, Security Cameras, Audio/ Video Power Supply, 3D Printer, LED Driver, CCTV Security System, LCD Monitor, Webcam Router and other 12V devices. This\xa0product\xa0is\xa0an\xa0AC\xa0to\xa0DC\xa0adapter,\xa0NOT\xa0charger!!!It\xa0can’t\xa0be\xa0used\xa0to\xa0charge\xa0electronic\xa0devices\xa0with\xa0batteries,\xa0such\xa0as\xa0laptops.\xa0Otherwise,\xa0it\xa0may\xa0damage\xa0your\xa0device. If you don't like our product or don't want it, please feel free to contact us. We will happily accept the return and give you refund, we have 30 days money back guarantee, 18 months exchange.", image='https://m.media-amazon.com/images/I/61qDGTATaLL.__AC_SX300_SY300_QL70_FMwebp_.jpg', answered_questions=0, date_first_available='May 10, 2019', model_number='SNT-1206', manufacturer='SHNITPWR', department='null', plus_content='true', upc='null', video='false'),
     Row(timestamp=datetime.date(2023, 8, 15), asin='B09871LZYT', brand='FINTIE', buybox_seller='Fintie', final_price='15.99', number_of_sellers=2, root_bs_rank=12194, reviews_count=1576, currency='USD', image_url='https://m.media-amazon.com/images/I/81Adh+cGYcS._AC_SY300_SX300_.jpg', images_count=1, title='Fintie Silicone Case for All-New Fire HD 10 and Fire HD 10 Plus Tablet (Only compatible with 11th Generation 2021 Release) - [Honey Comb] Light Weight Shock Proof Back Cover, Black', url='https://www.amazon.com/Fintie-Silicone-All-New-Compatible-Generation/dp/B08YYQ7SHQ', video_count=0, categories='Electronics,Computers & Accessories,Tablet Accessories,Bags, Cases & Sleeves,Cases', item_weight='7.4 ounces', rating=4.7, seller_id='A3A3E6QGUGPEMU', availability='In Stock', product_dimensions='5.12 x 1.77 x 0.67 inches', discount='-52%', initial_price='32.99', description='Specifically designed for All-New Amazon Fire HD 10 Tablet and Fire HD 10 Plus Tablet (Compatible with 11th Generation, 2021 Release). Special rear pattern with raised supports for grip and drop protection in a Patented Honey Comb anti slip design. Made of duarable impact-resistant silicone. The 2nd generation Honey Comb series provide extra corner protection with enhanced shock absorber design to protect from extreme shock and impact. All ports, buttons and speakers have precision cut -outs for easy access. Form fit feature protects back and sides from scratches, dirt and bumps. Supports Fire HD 10 Plus 2021 11th generation tablet wireless charging with Amazon Wireless Charging Dock. Available in a variety of bright, fun colors.', image='https://m.media-amazon.com/images/I/81Adh+cGYcS._AC_SY300_SX300_.jpg', answered_questions=0, date_first_available='March 15, 2021', model_number='EKAR025', manufacturer='FINTIE', department='null', plus_content='true', upc='null', video='false'),
     Row(timestamp=datetime.date(2023, 8, 16), asin='B09W9BXT9Z', brand='Nixplay', buybox_seller='Amazon.com', final_price='219.99', number_of_sellers=3, root_bs_rank=366, reviews_count=1998, currency='USD', image_url='https://m.media-amazon.com/images/I/71iSS-r+KJL._AC_SY300_SX300_.jpg', images_count=1, title='Nixplay 10.1 inch Touch Screen Digital Picture Frame with WiFi (W10K) - Polished Steel - Share Photos and Videos Instantly via Email or App - Preload Content', url='https://www.amazon.com/Nixplay-Digital-W10K-Polished-Instantly/dp/B09W9BXT9Z', video_count=0, categories='Electronics,Camera & Photo,Lighting & Studio,Photo Studio,Storage & Presentation Materials,Digital Picture Frames', item_weight='1.63 pounds', rating=4.6, seller_id='ATVPDKIKX0DER', availability='Only 5 left in stock - order soon', product_dimensions='10.55 x 4.65 x 0.99 inches', discount='-25%', initial_price='219.99', description='Nixplay Digital Picture Frame with Touch Screen. Share your moments through the Nixplay mobile app for iOS/Android and make the most of your most treasured memories.', image='https://m.media-amazon.com/images/I/71iSS-r+KJL._AC_SY300_SX300_.jpg', answered_questions=0, date_first_available='April 6, 2022', model_number='W10K - Polished Steel', manufacturer='Nixplay', department='null', plus_content='false', upc='null', video='false'),
     Row(timestamp=datetime.date(2022, 10, 13), asin='B093F837T9', brand='Sound Storm Laboratories', buybox_seller='Speece, Inc.', final_price='$335.99', number_of_sellers=2, root_bs_rank=45741, reviews_count=324, currency='$', image_url='https://m.media-amazon.com/images/I/71DXeNXt84L.__AC_SX300_SY300_QL70_FMwebp_.jpg', images_count=5, title='Sound Storm Laboratories SDML10ACP Single Din Chassis, Detachable 10 Inch Capacitive Touchscreen, Apple CarPlay, Android Auto, Bluetooth, No DVD, RGB Illumination, High Resolution FLAC Audio', url='https://www.amazon.com/dp/B093F837T9?language=en_US&currency=USD', video_count=1, categories='Electronics,Car & Vehicle Electronics,Car Electronics,Car Video,In-Dash DVD & Video Receivers', item_weight='5.45 pounds', rating=4.1, seller_id='ABO907060G8YG', availability='In Stock.', product_dimensions='4.94 x 7.01 x 1.97 inches', discount='null', initial_price='null', description='Sound Storm Laboratories SDML10ACP Apple CarPlay Android Auto Car Multimedia Player – Single Din Chassis With 10 Inch Capacitive Touchscreen, Bluetooth, No DVD, High Resolution FLAC, RGB Illumination', image='https://m.media-amazon.com/images/I/71DXeNXt84L.__AC_SX300_SY300_QL70_FMwebp_.jpg', answered_questions=146, date_first_available='January 21, 2021', model_number='SDML10ACP', manufacturer='Sound Storm Laboratories', department='null', plus_content='true', upc='null', video='true'),
     Row(timestamp=datetime.date(2024, 2, 4), asin='B0C2YQ9BJ1', brand='Redragon', buybox_seller='Redragon Shop', final_price='39.99', number_of_sellers=1, root_bs_rank=91, reviews_count=8219, currency='USD', image_url='https://m.media-amazon.com/images/I/71chbo4DCqL.__AC_SX300_SY300_QL70_FMwebp_.jpg', images_count=1, title='Redragon GS520 RGB Desktop Speakers, 2.0 Channel PC Computer Stereo Speaker with 6 Colorful LED Modes, Enhanced Sound and Easy-Access Volume Control, USB Powered w/ 3.5mm Cable, Pink', url='https://www.amazon.com/Redragon-GS520-Speakers-Computer-Easy-Access/dp/B0C2YQ9BJ1?th=1&psc=1&currency=USD&language=en_GB', video_count=0, categories='Electronics,Computers & Accessories,Computer Accessories & Peripherals,Audio & Video Accessories,Computer Speakers', item_weight='0.071 ounces', rating=4.4, seller_id='A2FK9EP27A6ZE6', availability='In Stock', product_dimensions='7.36 x 4.33 x 6.69 inches', discount='-5%', initial_price='41.99', description='Plug & Play, Broad Compatibility USB powered with 3.5mm audio and mic cables allows this rockstar to party on wide types of stages. Up to 35 in (90 cm) cable length between two speakers.   Detailed Texture Style Minimalist, but not singularly boring. Urban modern style with brushed texture finish, highlighting your taste.', image='https://m.media-amazon.com/images/I/71chbo4DCqL.__AC_SX300_SY300_QL70_FMwebp_.jpg', answered_questions=0, date_first_available='August 24, 2020', model_number='GS520', manufacturer='Redragon', department='Electronics', plus_content='true', upc='null', video='false'),
     Row(timestamp=datetime.date(2024, 1, 21), asin='B08D3Y5PFZ', brand='AOC', buybox_seller='Amazon.com', final_price='129.99', number_of_sellers=18, root_bs_rank=2417, reviews_count=14803, currency='USD', image_url='https://m.media-amazon.com/images/I/71aXlu6n1qL.__AC_SX300_SY300_QL70_FMwebp_.jpg', images_count=1, title='AOC C24G1A 24" Curved Frameless Gaming Monitor, FHD 1920x1080, 1500R, VA, 1ms MPRT, 165Hz (144Hz supported), FreeSync Premium, Height adjustable Black', url='https://www.amazon.com/AOC-C24G1A-Frameless-1920x1080-adjustable/dp/B08D3Y5PFZ?th=1&psc=1&currency=USD&language=en_GB', video_count=0, categories='Electronics,Computers & Accessories,Monitors', item_weight='9.92 pounds', rating=4.7, seller_id='ATVPDKIKX0DER', availability='In Stock', product_dimensions='9.64 x 21.14 x 20.19 inches', discount='-13%', initial_price='149.99', description='AOC C24G1A 24-inch Class Curved Gaming Monitor FreeSync Premium support ensures a stutter-free and tear-free gaming experience at any frame rate. Equipped with a curved 23.6 VA panel, the C24G1A displays a super-detailed Full HD resolution (1920x1080 pixels) at a screaming 165Hz refresh rate and 1ms response time (MPRT).Crazy Immersive ExperienceIts extremely curved design (1500R curvature) wraps around you, putting you at the center of the action and providing an immersive gaming experience. Wrap yourself in a pair (or three) of these frameless monitors for the ultimate curved viewing experience.165Hz, 1ms Gameplay165Hz refresh rate and 1ms response times brings unprecedented smoothness and fluidity to your games and virtual instantaneous response to your mouse and keyboard actions. With LFC support that allows the monitor to adjust between 48-165Hz refresh rate, gone are the days of tearing and stutters. Maximize the performance of your consoles by unleashing up to 120Hz frame rate (exact performance depends on consoles) and ultra low latency, giving you an edge over your opponents.The Ultimate Battle Station expands your view with multiple monitor set-ups. Its frameless design (with narrow borders) offers minimal bezel distraction and an extra clean set-up for the ultimate battle station.AMD FreeSync PremiumAMD FreeSyncPremium synchronizes the display and AMD GPU in your PC to eliminate screen tearing, stutter and input lag, providing the smoothest, fastest, and visually stunning gaming experience possible.Height Adjustable Stand and Quick ReleaseThe AOC Gaming C24G1A monitor comes with a height adjustable stand and swivel and tilt movements to adjust it to the optimal position to promote better ergonomics, allowing for better sitting postures during extended gaming sessions. The quick release button on the stand allows for quick disassembly for times when you need to bring your rig to the next battle ground.What\'s in the Box: 23.6" monitor, power cord, HDMI cable, DP cable, and QSG. At AOC, we design great products that meet our customers’ needs in a sustainable and responsible way. Our entire lineup is Mercury-free, ROHS-compliant, and made from conflict-free materials. Our packaging now includes less plastic, less ink, and more paper. To learn more about our full commitment to a greener future, visit Environment Policy | AOC Monitors.', image='https://m.media-amazon.com/images/I/71aXlu6n1qL.__AC_SX300_SY300_QL70_FMwebp_.jpg', answered_questions=0, date_first_available='September 15, 2020', model_number='C24G1A', manufacturer='AOC', department='Electronics', plus_content='true', upc='null', video='false')]

</div>

</div>

<div class="cell code" execution_count="8"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:11.545803100Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:11.369572500Z&quot;}"
collapsed="false">

``` python
# Selecionando colunas específicas
df.select('brand','title','images_count','video_count').limit(5).toPandas()
```

<div class="output execute_result" execution_count="8">

                          brand  \
    0                  SHNITPWR   
    1                    FINTIE   
    2                   Nixplay   
    3  Sound Storm Laboratories   
    4                  Redragon   

                                                   title  images_count  \
    0  SHNITPWR 12V 6A AC DC Power Supply Adapter Con...             7   
    1  Fintie Silicone Case for All-New Fire HD 10 an...             1   
    2  Nixplay 10.1 inch Touch Screen Digital Picture...             1   
    3  Sound Storm Laboratories SDML10ACP Single Din ...             5   
    4  Redragon GS520 RGB Desktop Speakers, 2.0 Chann...             1   

       video_count  
    0            0  
    1            0  
    2            0  
    3            1  
    4            0  

</div>

</div>

<div class="cell code" execution_count="9"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:12.548283200Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:11.546802400Z&quot;}"
collapsed="false">

``` python
# Verificando se o ASIN (Amazon Standard Identification Number) é único (UNIQUE)
# O método collect() retona uma lista de linhas.[0] é usado para acessar a primeira linha na lista,
# e [0] de novo é usado para acessar o primeiro elemento (a contagem de valores distintos) na linha

df.agg(countDistinct("asin")).collect()[0][0]
```

<div class="output execute_result" execution_count="9">

    1000

</div>

</div>

<div class="cell code" execution_count="10"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:12.831704200Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:12.553284900Z&quot;}"
collapsed="false">

``` python
# Outra alternativa é agrupar e contar todas as linhas 
(df.groupby('asin').count()).count()
```

<div class="output execute_result" execution_count="10">

    1000

</div>

</div>

<div class="cell code" execution_count="11"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:13.327804Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:12.835705700Z&quot;}"
collapsed="false">

``` python
# Filtrando o DataFrame para uma marca específica 
df.filter(col('brand') == 'SanDisk').limit(3).toPandas()
```

<div class="output execute_result" execution_count="11">

        timestamp        asin    brand buybox_seller final_price  \
    0  2024-02-04  B09X7DY7Q4  SanDisk  CalvinNHobbs       13.25   
    1  2024-01-16  B07SSZ8W38  SanDisk    Amazon.com           9   
    2  2023-08-25  B0000D8CMK  SanDisk        espee7       25.93   

       number_of_sellers  root_bs_rank  reviews_count currency  \
    0                 19            44           9779      USD   
    1                 11           227           6204      USD   
    2                  4           482            175      USD   

                                               image_url  images_count  \
    0  https://m.media-amazon.com/images/I/814l26oR+7...             1   
    1  https://m.media-amazon.com/images/I/51jPauko63...             1   
    2  https://m.media-amazon.com/images/I/51pU5lLQcW...             1   

                                                   title  \
    0  SanDisk 64GB Extreme SDXC UHS-I Memory Card - ...   
    1  SanDisk 64GB Ultra Luxe USB 3.1 Gen 1 Flash Dr...   
    2       SanDisk SD - Flash Memory Card - 512 MB - SD   

                                                     url  video_count  \
    0  https://www.amazon.com/SanDisk-Extreme-UHS-I-M...            0   
    1  https://www.amazon.com/SanDisk-64GB-Ultra-Flas...            0   
    2  https://www.amazon.com/SanDisk-SD-Flash-Memory...            0   

                                              categories   item_weight  rating  \
    0  Electronics,Computers & Accessories,Computer A...  0.071 ounces     4.8   
    1  Electronics,Computers & Accessories,Data Stora...   0.05 ounces     4.5   
    2  Electronics,Computers & Accessories,Computer A...  0.071 ounces     4.2   

            seller_id                        availability  \
    0  A2NDNAPHQ3UDKH  Only 2 left in stock - order soon.   
    1   ATVPDKIKX0DER                            In Stock   
    2  A2OR3WSDZ5KM01                            In Stock   

              product_dimensions discount initial_price  \
    0  0.09 x 0.94 x 1.26 inches     -12%         14.99   
    1  1.57 x 0.62 x 0.23 inches     -50%         17.99   
    2  0.94 x 1.26 x 0.08 inches     -42%         44.95   

                                             description  \
    0  With the SanDisk Extreme SD UHS-I memory card ...   
    1  Combine the necessity for a convenient way to ...   
    2  SanDisk Corporation is the world's largest sup...   

                                                   image  answered_questions  \
    0  https://m.media-amazon.com/images/I/814l26oR+7...                   0   
    1  https://m.media-amazon.com/images/I/51jPauko63...                   0   
    2  https://m.media-amazon.com/images/I/51pU5lLQcW...                   0   

      date_first_available        model_number  \
    0        June 16, 2022  SDSDXV2-064G-GNCIN   
    1        June 28, 2019     SDCZ74-064G-A46   
    2        July 20, 2007       SDSDB-512-E10   

                             manufacturer   department plus_content   upc  video  
    0  Western Digital Technologies, Inc.  Electronics         true  null  false  
    1  Western Digital Technologies, Inc.  Electronics         true  null  false  
    2                 SanDisk Corporation         null        false  null  false  

</div>

</div>

<div class="cell code" execution_count="12"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:13.441790500Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:13.327804Z&quot;}"
collapsed="false">

``` python
# Filtrando o DataFrame por produtos com mais de 5 imagens
df.filter(col('images_count') > 5).select('asin','title','brand','images_count').limit(5).toPandas()
```

<div class="output execute_result" execution_count="12">

             asin                                              title      brand  \
    0  B07RGHBLGC  SHNITPWR 12V 6A AC DC Power Supply Adapter Con...   SHNITPWR   
    1  B016YJWJ6S  Elgato Game Capture HD60 - Next Generation Gam...    Corsair   
    2  B01EKARUUS  StarTech.com USB-C Multiport Adapter - USB-C T...   StarTech   
    3  B0875NRKTP  Tz Tape 12mm 0.47 Laminated Clear Replacement ...  COLORWING   
    4  B08B34C5Y7  GearIT HDMI Cable (10-Pack / 6.6ft / 2m) High-...     GearIT   

       images_count  
    0             7  
    1             6  
    2             9  
    3             8  
    4             7  

</div>

</div>

<div class="cell code" execution_count="13"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:13.525867600Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:13.401587600Z&quot;}"
collapsed="false">

``` python
 # Filtrando o DataFrame por produtos com mais de 5 imagens AND mais de 5 videos
df.filter((col('images_count') > 5) & (col('video_count') > 5)).select('asin','title','brand','images_count','video_count').limit(5).toPandas()
```

<div class="output execute_result" execution_count="13">

             asin                                              title     brand  \
    0  B09BDDSDTC  Sony SRS-XG500 X-Series Wireless Portable Blue...      Sony   
    1  B08QCLVJBM  Leayjeen Kids Digital Video Camera Case Compat...  Leayjeen   
    2  B087NHNVT5  Linklike Wired Earbuds with Microphone Extra B...  Linklike   
    3  B09PFNNQ9N  FRAMEO Digital Photo Frame 10.1 inch WiFi Smar...    dxmart   
    4  B0921ZK2FK  Leayjeen Kids Digital Video Camera Case Compat...  Leayjeen   

       images_count  video_count  
    0            14            6  
    1             6            6  
    2             8            6  
    3             9            6  
    4             6            6  

</div>

</div>

<div class="cell markdown" collapsed="false">

## Limpeza e Transformação de Dados

</div>

<div class="cell markdown" collapsed="false">

### Mantendo apenas as colunas de interesse

</div>

<div class="cell code" execution_count="14"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:13.543096800Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:13.472387200Z&quot;}"
collapsed="false">

``` python
cols = ['timestamp', 'asin', 'brand', 'initial_price', 'final_price', 'reviews_count', 'images_count', 'title', 'url', 'video_count', 'rating', 'categories']

df = df.select(cols)
```

</div>

<div class="cell code" execution_count="15"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:13.647820800Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:13.489798800Z&quot;}"
collapsed="false">

``` python
df.limit(1000).toPandas()
```

<div class="output execute_result" execution_count="15">

          timestamp        asin                     brand initial_price  \
    0    2023-08-03  B07RGHBLGC                  SHNITPWR         23.99   
    1    2023-08-15  B09871LZYT                    FINTIE         32.99   
    2    2023-08-16  B09W9BXT9Z                   Nixplay        219.99   
    3    2022-10-13  B093F837T9  Sound Storm Laboratories          null   
    4    2024-02-04  B0C2YQ9BJ1                  Redragon         41.99   
    ..          ...         ...                       ...           ...   
    995  2023-09-11  B0076SIH04          Rockford Fosgate        209.99   
    996  2023-08-23  B002L5WCLM              StarTech.com         17.99   
    997  2024-01-16  B07XBWHR56                SUPERNIGHT         15.99   
    998  2024-01-15  B017JIHJIG                  Verbatim            12   
    999  2023-08-25  B08CY3GPL4                    YFFIZQ         59.99   

        final_price  reviews_count  images_count  \
    0         13.99            234             7   
    1         15.99           1576             1   
    2        219.99           1998             1   
    3       $335.99            324             5   
    4         39.99           8219             1   
    ..          ...            ...           ...   
    995      189.99            100             1   
    996       13.89            419             1   
    997       15.99           1227             1   
    998        6.97           1374             1   
    999       49.99           1589             1   

                                                     title  \
    0    SHNITPWR 12V 6A AC DC Power Supply Adapter Con...   
    1    Fintie Silicone Case for All-New Fire HD 10 an...   
    2    Nixplay 10.1 inch Touch Screen Digital Picture...   
    3    Sound Storm Laboratories SDML10ACP Single Din ...   
    4    Redragon GS520 RGB Desktop Speakers, 2.0 Chann...   
    ..                                                 ...   
    995  Rockford Fosgate P3SD2-10 Punch P3S 10" 2-Ohm ...   
    996  StarTech.com 7ft CAT6a Ethernet Cable - 10 Gig...   
    997  DC 12V Step Up to 24V 3A Boost Converter 72W D...   
    998  Verbatim 16GB Metal Executive USB Flash Drive ...   
    999  YFFIZQ 80GB MP3 Player with Bluetooth 5.1,2.8'...   

                                                       url  video_count  rating  \
    0    https://www.amazon.com/SHNITPWR-Converter-100V...            0     4.4   
    1    https://www.amazon.com/Fintie-Silicone-All-New...            0     4.7   
    2    https://www.amazon.com/Nixplay-Digital-W10K-Po...            0     4.6   
    3    https://www.amazon.com/dp/B093F837T9?language=...            1     4.1   
    4    https://www.amazon.com/Redragon-GS520-Speakers...            0     4.4   
    ..                                                 ...          ...     ...   
    995  https://www.amazon.com/Rockford-Fosgate-P3SD2-...            0     4.2   
    996  https://www.amazon.com/StarTech-com-Cat6a-Ethe...            0     4.7   
    997  https://www.amazon.com/24V-Boost-Converter-Reg...            0     4.5   
    998  https://www.amazon.com/Verbatim-Metal-Executiv...            0     4.6   
    999  https://www.amazon.com/Bluetooth-Portable-Spea...            0     3.8   

                                                categories  
    0            Electronics,Power Accessories,AC Adapters  
    1    Electronics,Computers & Accessories,Tablet Acc...  
    2    Electronics,Camera & Photo,Lighting & Studio,P...  
    3    Electronics,Car & Vehicle Electronics,Car Elec...  
    4    Electronics,Computers & Accessories,Computer A...  
    ..                                                 ...  
    995  Electronics,Car & Vehicle Electronics,Car Elec...  
    996  Electronics,Computers & Accessories,Computer A...  
    997     Electronics,Power Accessories,Power Converters  
    998  Electronics,Computers & Accessories,Data Stora...  
    999  Electronics,Portable Audio & Video,MP3 & MP4 P...  

    [1000 rows x 12 columns]

</div>

</div>

<div class="cell markdown" collapsed="false">

### Quantos valores null temos no DataFrame?

</div>

<div class="cell code" execution_count="16"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:13.881089800Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:13.607649200Z&quot;}"
collapsed="false">

``` python
null_counts = df.select([count(when(col(c).isNull() | col(c).isin('null','Null','NULL','NaN', 'NAN', ' ',''), c)).alias(c) for c in df.columns])

null_counts.toPandas()
```

<div class="output execute_result" execution_count="16">

       timestamp  asin  brand  initial_price  final_price  reviews_count  \
    0          0     0      0            304            0              0   

       images_count  title  url  video_count  rating  categories  
    0             0      0    0            0       0           0  

</div>

</div>

<div class="cell code" execution_count="17"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:14.574925500Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:13.885080600Z&quot;}"
collapsed="false">

``` python
transposed_df = null_counts.toPandas().T.reset_index()

# Renomeando as colunas
transposed_df.columns = ['column','value']

# Definindo do tamanho da figura para o gráfico
plt.figure(figsize = (15,6))

# Criando gráfico de barras horizontais usando Seaborn com o figure size customizado
ax = sns.barplot(x = 'value', y = 'column', data = transposed_df)
sns.set_palette('dark')

# Título do gráfico
plt.title('Missing Values')

# Inserindo Bar Data Labels
for p in ax.patches:
    width = p.get_width()
    value = '{:,.0f}'.format(width)
    x = width + 0.1
    y = p.get_y() + p.get_height() / 2 + 0.1
    ax.annotate(value, (x, y), fontsize = 8)

# Exibindo o gráfico
plt.show()
```

<div class="output display_data">

![](f26573587b8b2e4509b6acd068226fbf4275ca4a.png)

</div>

</div>

<div class="cell markdown" collapsed="false">

### Limpeza e transformação de dados: colunas (initial_price & final_price)

</div>

<div class="cell code" execution_count="18"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:14.648530700Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:14.534788800Z&quot;}"
collapsed="false">

``` python
# Lista de colunas para aplicar a transformação
columns = ['initial_price', 'final_price']

# Selecionando o primeiro preço se existem dois preços listados
for column in columns:
    df = df.withColumn(column, split(col(column), " ").getItem(0))

# Drop de linhas onde o preço está faltando
df = df.where(~df.initial_price.isin('null','Null','NULL','NaN', 'NAN', ' ',''))
```

</div>

<div class="cell code" execution_count="19"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:14.680071600Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:14.565831900Z&quot;}"
collapsed="false">

``` python
# Verificando novamente se não há valores ausentes nas colunas
null_counts = df.select([count(when(col(c).isNull() | col(c).isin('null','Null','NULL','NaN', 'NAN', ' ',''), c)).alias(c) for c in df.columns[3:5]])

null_counts.toPandas()
```

<div class="output execute_result" execution_count="19">

       initial_price  final_price
    0              0            0

</div>

</div>

<div class="cell code" execution_count="20"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:14.785077800Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:14.682072800Z&quot;}"
collapsed="false">

``` python
# Removendo "$" das colunas de preço e mudando o data type de string para double
for column in columns:
    df = df.withColumn(column, regexp_replace(col(column), "[US$,]","").cast('double'))
```

</div>

<div class="cell code" execution_count="21"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:14.868542700Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:14.714635500Z&quot;}"
collapsed="false">

``` python
df.limit(1000).toPandas()
```

<div class="output execute_result" execution_count="21">

          timestamp        asin             brand  initial_price  final_price  \
    0    2023-08-03  B07RGHBLGC          SHNITPWR          23.99        13.99   
    1    2023-08-15  B09871LZYT            FINTIE          32.99        15.99   
    2    2023-08-16  B09W9BXT9Z           Nixplay         219.99       219.99   
    3    2024-02-04  B0C2YQ9BJ1          Redragon          41.99        39.99   
    4    2024-01-21  B08D3Y5PFZ               AOC         149.99       129.99   
    ..          ...         ...               ...            ...          ...   
    691  2023-09-11  B0076SIH04  Rockford Fosgate         209.99       189.99   
    692  2023-08-23  B002L5WCLM      StarTech.com          17.99        13.89   
    693  2024-01-16  B07XBWHR56        SUPERNIGHT          15.99        15.99   
    694  2024-01-15  B017JIHJIG          Verbatim          12.00         6.97   
    695  2023-08-25  B08CY3GPL4            YFFIZQ          59.99        49.99   

         reviews_count  images_count  \
    0              234             7   
    1             1576             1   
    2             1998             1   
    3             8219             1   
    4            14803             1   
    ..             ...           ...   
    691            100             1   
    692            419             1   
    693           1227             1   
    694           1374             1   
    695           1589             1   

                                                     title  \
    0    SHNITPWR 12V 6A AC DC Power Supply Adapter Con...   
    1    Fintie Silicone Case for All-New Fire HD 10 an...   
    2    Nixplay 10.1 inch Touch Screen Digital Picture...   
    3    Redragon GS520 RGB Desktop Speakers, 2.0 Chann...   
    4    AOC C24G1A 24" Curved Frameless Gaming Monitor...   
    ..                                                 ...   
    691  Rockford Fosgate P3SD2-10 Punch P3S 10" 2-Ohm ...   
    692  StarTech.com 7ft CAT6a Ethernet Cable - 10 Gig...   
    693  DC 12V Step Up to 24V 3A Boost Converter 72W D...   
    694  Verbatim 16GB Metal Executive USB Flash Drive ...   
    695  YFFIZQ 80GB MP3 Player with Bluetooth 5.1,2.8'...   

                                                       url  video_count  rating  \
    0    https://www.amazon.com/SHNITPWR-Converter-100V...            0     4.4   
    1    https://www.amazon.com/Fintie-Silicone-All-New...            0     4.7   
    2    https://www.amazon.com/Nixplay-Digital-W10K-Po...            0     4.6   
    3    https://www.amazon.com/Redragon-GS520-Speakers...            0     4.4   
    4    https://www.amazon.com/AOC-C24G1A-Frameless-19...            0     4.7   
    ..                                                 ...          ...     ...   
    691  https://www.amazon.com/Rockford-Fosgate-P3SD2-...            0     4.2   
    692  https://www.amazon.com/StarTech-com-Cat6a-Ethe...            0     4.7   
    693  https://www.amazon.com/24V-Boost-Converter-Reg...            0     4.5   
    694  https://www.amazon.com/Verbatim-Metal-Executiv...            0     4.6   
    695  https://www.amazon.com/Bluetooth-Portable-Spea...            0     3.8   

                                                categories  
    0            Electronics,Power Accessories,AC Adapters  
    1    Electronics,Computers & Accessories,Tablet Acc...  
    2    Electronics,Camera & Photo,Lighting & Studio,P...  
    3    Electronics,Computers & Accessories,Computer A...  
    4         Electronics,Computers & Accessories,Monitors  
    ..                                                 ...  
    691  Electronics,Car & Vehicle Electronics,Car Elec...  
    692  Electronics,Computers & Accessories,Computer A...  
    693     Electronics,Power Accessories,Power Converters  
    694  Electronics,Computers & Accessories,Data Stora...  
    695  Electronics,Portable Audio & Video,MP3 & MP4 P...  

    [696 rows x 12 columns]

</div>

</div>

<div class="cell code" execution_count="22"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:14.881325900Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:14.867649800Z&quot;}"
collapsed="false">

``` python
# Checando schema
df.printSchema()
```

<div class="output stream stdout">

    root
     |-- timestamp: date (nullable = true)
     |-- asin: string (nullable = true)
     |-- brand: string (nullable = true)
     |-- initial_price: double (nullable = true)
     |-- final_price: double (nullable = true)
     |-- reviews_count: integer (nullable = true)
     |-- images_count: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- url: string (nullable = true)
     |-- video_count: integer (nullable = true)
     |-- rating: double (nullable = true)
     |-- categories: string (nullable = true)

</div>

</div>

<div class="cell markdown" collapsed="false">

### Limpeza da coluna rating

</div>

<div class="cell code" execution_count="23"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:14.892186300Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:14.874331900Z&quot;}"
collapsed="false">

``` python
df = df.na.drop(subset = ['rating'])
```

</div>

<div class="cell code" execution_count="24"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:15.029493Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:14.891184100Z&quot;}"
collapsed="false">

``` python
# Verificando novamente se não há valores ausentes na coluna rating
df.filter(col('rating').isNull()).count()
```

<div class="output execute_result" execution_count="24">

    0

</div>

</div>

<div class="cell markdown" collapsed="false">

### Selecionando a categoria detalhada da coluna categories (ou seja, o último elemento da lista, a descrição mais granular da categoria)

</div>

<div class="cell code" execution_count="25"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:15.036497200Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:14.990719100Z&quot;}"
collapsed="false">

``` python
# Criando coluna array categoria para assim extrairmos o último elemento
df = df.withColumn('cat_array', split(df.categories,','))
```

</div>

<div class="cell code" execution_count="26"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:15.104977700Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:15.010394400Z&quot;}"
collapsed="false">

``` python
# Criando a coluna categoria detalhada
df = df.withColumn('detailed_category', df.cat_array[size(df.cat_array)-1])
```

</div>

<div class="cell code" execution_count="27"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:15.122977Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:15.031499400Z&quot;}"
collapsed="false">

``` python
pd.options.display.max_colwidth = None
```

</div>

<div class="cell code" execution_count="28"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:15.178545Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:15.051834400Z&quot;}"
collapsed="false">

``` python
df.select('brand','title','categories','detailed_category').toPandas()
```

<div class="output execute_result" execution_count="28">

                    brand  \
    0            SHNITPWR   
    1              FINTIE   
    2             Nixplay   
    3            Redragon   
    4                 AOC   
    ..                ...   
    691  Rockford Fosgate   
    692      StarTech.com   
    693        SUPERNIGHT   
    694          Verbatim   
    695            YFFIZQ   

                                                                                                                                                                                                            title  \
    0    SHNITPWR 12V 6A AC DC Power Supply Adapter Converter 100V~240V AC to DC 12 Volt 6 Amp 72W LED Driver Transformer with 5.5x2.5mm Plug for 5050 3528 LED Strip 3D Printer CCTV Security System LCD Monitor   
    1                        Fintie Silicone Case for All-New Fire HD 10 and Fire HD 10 Plus Tablet (Only compatible with 11th Generation 2021 Release) - [Honey Comb] Light Weight Shock Proof Back Cover, Black   
    2                                               Nixplay 10.1 inch Touch Screen Digital Picture Frame with WiFi (W10K) - Polished Steel - Share Photos and Videos Instantly via Email or App - Preload Content   
    3                      Redragon GS520 RGB Desktop Speakers, 2.0 Channel PC Computer Stereo Speaker with 6 Colorful LED Modes, Enhanced Sound and Easy-Access Volume Control, USB Powered w/ 3.5mm Cable, Pink   
    4                                                      AOC C24G1A 24" Curved Frameless Gaming Monitor, FHD 1920x1080, 1500R, VA, 1ms MPRT, 165Hz (144Hz supported), FreeSync Premium, Height adjustable Black   
    ..                                                                                                                                                                                                        ...   
    691                                                                                                                                       Rockford Fosgate P3SD2-10 Punch P3S 10" 2-Ohm DVC Shallow Subwoofer   
    692       StarTech.com 7ft CAT6a Ethernet Cable - 10 Gigabit Shielded Snagless RJ45 100W PoE Patch Cord - 10GbE STP Network Cable w/Strain Relief - Blue Fluke Tested/Wiring is UL Certified/TIA (C6ASPAT7BL)   
    693                                                                    DC 12V Step Up to 24V 3A Boost Converter 72W DC Voltage Regulator Power Converter Waterproof Module Transformer for Golf Cart Club Car   
    694                                                                                                                                            Verbatim 16GB Metal Executive USB Flash Drive - Silver - 98748   
    695         YFFIZQ 80GB MP3 Player with Bluetooth 5.1,2.8'' Full Touch Screen MP4 Player with Speaker,Portable HiFi Lossless Sound MP3 Player with FM Radio,Voice Recorder,E-Book,Armband,Support up to 128GB   

                                                                                                                                              categories  \
    0                                                                                                          Electronics,Power Accessories,AC Adapters   
    1                                                                 Electronics,Computers & Accessories,Tablet Accessories,Bags, Cases & Sleeves,Cases   
    2                                  Electronics,Camera & Photo,Lighting & Studio,Photo Studio,Storage & Presentation Materials,Digital Picture Frames   
    3                                 Electronics,Computers & Accessories,Computer Accessories & Peripherals,Audio & Video Accessories,Computer Speakers   
    4                                                                                                       Electronics,Computers & Accessories,Monitors   
    ..                                                                                                                                               ...   
    691                                                  Electronics,Car & Vehicle Electronics,Car Electronics,Car Audio,Subwoofers,Component Subwoofers   
    692  Electronics,Computers & Accessories,Computer Accessories & Peripherals,Cables & Accessories,Cables & Interconnects,Ethernet Cables,Cat 6 Cables   
    693                                                                                                   Electronics,Power Accessories,Power Converters   
    694                                                                                Electronics,Computers & Accessories,Data Storage,USB Flash Drives   
    695                                                                                             Electronics,Portable Audio & Video,MP3 & MP4 Players   

              detailed_category  
    0               AC Adapters  
    1                     Cases  
    2    Digital Picture Frames  
    3         Computer Speakers  
    4                  Monitors  
    ..                      ...  
    691    Component Subwoofers  
    692            Cat 6 Cables  
    693        Power Converters  
    694        USB Flash Drives  
    695       MP3 & MP4 Players  

    [696 rows x 4 columns]

</div>

</div>

<div class="cell code" execution_count="29"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:15.415766600Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:15.180667Z&quot;}"
collapsed="false">

``` python
# Contagem de valores distintos na coluna detailed_category
(df.groupBy('detailed_category').count()).count()
```

<div class="output execute_result" execution_count="29">

    224

</div>

</div>

<div class="cell code" execution_count="30"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:15.603550Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:15.415766600Z&quot;}"
collapsed="false">

``` python
# Exibindo todas as categorias detalhadas
(df.groupBy('detailed_category').count()).toPandas()
```

<div class="output execute_result" execution_count="30">

               detailed_category  count
    0          MP3 & MP4 Players      3
    1                  Lens Caps      2
    2             Screen Filters      1
    3                   Headsets      1
    4           Power Converters      4
    ..                       ...    ...
    219                Eyepieces      1
    220         Complete Tripods      1
    221  Security & Surveillance      1
    222        Screen Protectors      2
    223               Amplifiers      1

    [224 rows x 2 columns]

</div>

</div>

<div class="cell markdown" collapsed="false">

## <span style='color:Yellow'> Obtendo os insights e Respondendo às perguntas-chave </span>

</div>

<div class="cell markdown" collapsed="false">

### <span style='color:Yellow'> Quais categorias detalhadas possuem mais produtos ? </span>

</div>

<div class="cell code" execution_count="31"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:16.681434500Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:15.602551200Z&quot;}"
collapsed="false">

``` python
# Group by 'detailed_category' e count do número de linhas de cada categoria
grouped_df = df.groupBy('detailed_category').agg(count("*").alias("count"))

# Order by contagem de categorias em ordem decrescente e com limite de 30 
top30_df = grouped_df.orderBy(col("count").desc()).limit(30).toPandas()

# Criando gráfico de barras horizontais com Seaborn
plt.figure(figsize=(12,8))
ax = sns.barplot(x = "count", y = "detailed_category", hue = "detailed_category", data = top30_df, palette = "viridis")
ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x,_:'{:,}'.format(x)))

# Bar Data Labels
for index, value in enumerate(top30_df["count"]):
    ax.text(value, index, f"{value:,.0f}", ha='left', va='center', color='black', fontsize=12)

ax.set_xlabel("Number of Products")
ax.set_ylabel("Detailed Category")
ax.set_title("Top 30 detailed categories with the most products")

plt.tight_layout()
plt.show()
```

<div class="output display_data">

![](e3f491c18e9460f8c1b78a489f51e296ae96fa45.png)

</div>

</div>

<div class="cell markdown" collapsed="false">

### <span style='color:Yellow'> Análises em específicas categorias detalhadas </span>

</div>

<div class="cell code" execution_count="32"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:17.665262800Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:16.698425500Z&quot;}"
collapsed="false">

``` python
# Definindo as variáveis a serem especificadas 
det_cat = 'Cases'
metric1 = 'rating'
metric2 = 'final_price'

# Filtro DataFrame para manter apenas as linhas onde categoria detalhada
filtered_df = df.filter(col("detailed_category") == det_cat)

# Group by 'brand' e contagem do número de linhas de cada marca
grouped_df = filtered_df.groupBy("brand").agg(count("*").alias("brand_count"))

# Cálculo de média rating para cada marca
avg_metric1_df = filtered_df.groupBy("brand").agg(avg(metric1).alias("avg_rating"))

# Cálculo de média final_price para cada marca
avg_metric2_df = filtered_df.groupBy("brand").agg(avg(metric2).alias("avg_final_price"))

# Join dos três DataFrames com marca ("brand") como parâmetro para as combinar informações
pandas_df = grouped_df.join(avg_metric1_df, on = "brand", how = "inner").join(avg_metric2_df, on = "brand", how = "inner").toPandas()

# Ordenando o DataFrame em ordem decrescente baseado na contagem de marca e rating
pandas_df = pandas_df.sort_values(by = ["brand_count", "avg_rating"], ascending = False)

# Limitando o resultado para as top 10 marcas por contagem e rating
pandas_df = pandas_df.head(10)

# Criando figura com três subplots
fig, axes = plt.subplots(1, 3, figsize=(20, 8))

# Plot do PRIMEIRO gráfico de barras horizontais por contagem de marca (brand_count)
sns.barplot(x="brand_count", y = "brand", hue = "brand", data = pandas_df, palette="viridis", ax=axes[0])

# Definindo pequeno offset para os data labels
label_offset = 0.1

# Bar Data Labels brand_count
for index, value in enumerate(pandas_df["brand_count"]):
    axes[0].text(value + label_offset, index, f"{value:,.0f}", ha='left', va='center', color='black', fontsize=12)

# Formatando labels eixo-x brand_count
axes[0].xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: '{:,.0f}'.format(x)))

# Definindo labels e título para o PRIMEIRO plot
axes[0].set_xlabel("Number of Products")
axes[0].set_ylabel("Brand")
axes[0].set_title("Top 10 Brands with most products in " + det_cat + " category")

# Plot do SEGUNDO gráfico de barras horizontais por média rating
sns.barplot(x="avg_rating", y="brand", hue="brand", data=pandas_df, palette="viridis", ax=axes[1])

# Bar Data Labels avg_rating 
for index, value in enumerate(pandas_df["avg_rating"]):
    axes[1].text(value + label_offset, index, f"{value:.1f}", ha='left', va='center', color='black', fontsize=12)

# Definindo labels e título para o SEGUNDO plot
axes[1].set_xlabel("Average " + metric1)
axes[1].set_ylabel("")  # Escondendo ylabel do SEGUNDO plot
axes[1].set_title("Average " + metric1 + " for Top 10 Brands in " + det_cat + " category")

# Plot do TERCEIRO gráfico de barras horizontais por média final_price
sns.barplot(x="avg_final_price", y="brand", hue="brand", data=pandas_df, palette="viridis", ax=axes[2])

# Bar Data Labels avg_final_price
for index, value in enumerate(pandas_df["avg_final_price"]):
    axes[2].text(value + label_offset, index, f"{value:.1f}", ha='left', va='center', color='black', fontsize=12)

# Definindo labels e título para o TERCEIRO plot
axes[2].set_xlabel("Average " + metric2)
axes[2].set_ylabel("")  # Escondendo ylabel do TERCEIRO plot
axes[2].set_title("Average " + metric2 + " for Top 10 Brands in " + det_cat + " category")

# Ajustando espaçamento entre subplots
plt.tight_layout()

# Exibindo gráficos
plt.show()
```

<div class="output display_data">

![](2253c04d7c69e3081ddc9bdf311fa5703b38ee22.png)

</div>

</div>

<div class="cell markdown" collapsed="false">

### <span style='color:Yellow'> Análises aprofundadas em marcas e produtos específicos </span>

</div>

<div class="cell code" execution_count="33"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:17.899097Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:17.674272100Z&quot;}"
collapsed="false">

``` python
#brands = ['SUPCASE', 'Spigen'] # Expensive (final_price alto)
#brands = ['Fintie', 'MoKo'] # Medium (final_price médio)
brands = ['GEEKRIA', 'Soke'] # Cheap (final_price baixo)

# Filtrando produtos das marcas específicas e ordenando por maior rating e maior reviews_count
(df.filter((col('brand').isin(brands)) & (col('detailed_category') == det_cat))
    .orderBy(['rating', 'reviews_count'], ascending=[0, 0])
    .toPandas())
```

<div class="output execute_result" execution_count="33">

        timestamp        asin    brand  initial_price  final_price  reviews_count  \
    0  2023-08-16  B093RY2NTW     Soke          19.99        19.99           2226   
    1  2023-09-10  B01N4IMMHC  GEEKRIA          14.99        14.99             78   
    2  2023-09-10  B01M3SDSYW  GEEKRIA          15.55        12.59             58   
    3  2023-09-10  B019XLBZXM  GEEKRIA          21.05        18.99            524   
    4  2023-08-16  B08Y5J3KLG     Soke          11.99         8.39            106   
    5  2023-09-10  B0771DBFHW  GEEKRIA           9.50         8.90             64   

       images_count  \
    0             1   
    1             1   
    2             1   
    3             1   
    4             1   
    5             1   

                                                                                                                                                                                                     title  \
    0  Soke iPad Pro 12.9 Case 2022 2021 with Pencil Holder - [Full Body Protection + 2nd Gen Apple Pencil Charge + Auto Wake/Sleep], Soft TPU Back Cover for iPad Pro 12.9 inch 6th 5th Generation(Black)   
    1                  Geekria Shield Headphones Case Compatible with Bose QC Ultra, QC45, NC 700, QC35, QC25, QC15, QC SE Case, Replacement Hard Shell Travel Carrying Bag with Cable Storage (Dark Grey)   
    2                Geekria Shield Case Compatible with Bose QC Ultra, 700, QC35 Gaming, QC35 II, QC35, QC SE Headphones, Replacement Protective Hard Shell Travel Carrying Bag with Cable Storage (Grey)   
    3                  Geekria Shield Headphones Case Compatible with Bang & Olufsen Beoplay H9i, H95, H9, H8, H8i, H6, H4 Case, Replacement Hard Shell Travel Carrying Bag with Cable Storage (Dark Grey)   
    4    Soke New iPad Pro 12.9 Case 5th Generation 2021, Premium Leather Stand Folio Case with Pocket[2nd Gen Apple Pencil Charging+Auto Wake/Sleep], Hard PC Back Cover for iPad Pro 12.9 Inch(Rosegold)   
    5         Geekria Shield Headphones Case Compatible with Skullcandy Ink'd+, Ink'd 2, Chops Flex in-Ear Sport Earbuds, Replacement Protective Hard Shell Travel Carrying Bag with Cable Storage (Black)   

                                                                                             url  \
    0                          https://www.amazon.com/Soke-iPad-12-9-Pencil-Holder/dp/B08Y5FG8XF   
    1  https://www.amazon.com/Geekria-UltraShell-Headphones-Protective-Accessories/dp/B01N4IMMHC   
    2  https://www.amazon.com/Geekria-UltraShell-Headphones-Protective-Accessories/dp/B01M3SDSYW   
    3      https://www.amazon.com/UltraShell-Headphones-OLUFSEN-SoundTrue-Carrying/dp/B019XLBZXM   
    4             https://www.amazon.com/Soke-Generation-Premium-Charging-Rosegold/dp/B09B57BL1N   
    5    https://www.amazon.com/Wireless-Bluetooth-Skullcandy-Carrying-Accessories/dp/B0771DBFHW   

       video_count  rating  \
    0            0     4.5   
    1            0     4.5   
    2            0     4.5   
    3            0     4.4   
    4            0     4.4   
    5            0     4.4   

                                                                               categories  \
    0  Electronics,Computers & Accessories,Tablet Accessories,Bags, Cases & Sleeves,Cases   
    1                                 Electronics,Headphones, Earbuds & Accessories,Cases   
    2                                 Electronics,Headphones, Earbuds & Accessories,Cases   
    3                                 Electronics,Headphones, Earbuds & Accessories,Cases   
    4  Electronics,Computers & Accessories,Tablet Accessories,Bags, Cases & Sleeves,Cases   
    5                                 Electronics,Headphones, Earbuds & Accessories,Cases   

                                                                                       cat_array  \
    0  [Electronics, Computers & Accessories, Tablet Accessories, Bags,  Cases & Sleeves, Cases]   
    1                                   [Electronics, Headphones,  Earbuds & Accessories, Cases]   
    2                                   [Electronics, Headphones,  Earbuds & Accessories, Cases]   
    3                                   [Electronics, Headphones,  Earbuds & Accessories, Cases]   
    4  [Electronics, Computers & Accessories, Tablet Accessories, Bags,  Cases & Sleeves, Cases]   
    5                                   [Electronics, Headphones,  Earbuds & Accessories, Cases]   

      detailed_category  
    0             Cases  
    1             Cases  
    2             Cases  
    3             Cases  
    4             Cases  
    5             Cases  

</div>

</div>

<div class="cell markdown" collapsed="false">

### <span style='color:Yellow'> Rating X Reviews </span>

</div>

<div class="cell code" execution_count="34"
ExecuteTime="{&quot;end_time&quot;:&quot;2024-02-20T16:58:18.197739700Z&quot;,&quot;start_time&quot;:&quot;2024-02-20T16:58:17.903110800Z&quot;}"
collapsed="false">

``` python
#brands = ['SUPCASE', 'Spigen'] # Expensive (final_price alto)
brands = ['Fintie', 'MoKo'] # Medium (final_price médio)
#brands = ['GEEKRIA', 'Soke'] # Cheap (final_price baixo)

# Filtrando produtos das marcas específicas
df_filtered = df.filter((col('brand').isin(brands)) & (col('detailed_category') == det_cat))

# Selecionando apenas as colunas de classificação e número de avaliações
rating_reviews_df = df_filtered.select('rating', 'reviews_count').toPandas()

# Plotando um scatterplot
plt.figure(figsize=(10, 6))
sns.scatterplot(data=rating_reviews_df, x='reviews_count', y='rating', hue='rating', palette='colorblind')

spacing = 10
# Adicionando labels  com as informações de reviews_count e rating em cada ponto
for index, row in rating_reviews_df.iterrows():
    plt.text(row['reviews_count'], row['rating'], f"{row['reviews_count']} / {row['rating']}", fontsize=10, ha='right', rotation=90)
    
# Customizando plot (título e labels)
plt.xlabel("Reviews Count")
plt.ylabel("Rating")
plt.title("Rating X Reviews Count", fontsize=15)

plt.tight_layout()

# Exibindo gráfico
plt.show()
```

<div class="output display_data">

![](b19b31e8ba296e9ce5b89cd22832c2215d21f0d2.png)

</div>

</div>
