
###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

###############################################################
# İş Problemi (Business Problem)
###############################################################
# FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranış öbeklenmelerine göre gruplar oluşturulacak..

###############################################################
# Veri Seti Hikayesi
###############################################################

# Veri seti son alışverişlerini 2020 - 2021 yıllarında OmniChannel(hem online hem offline alışveriş yapan) olarak yapan müşterilerin geçmiş alışveriş davranışlarından
# elde edilen bilgilerden oluşmaktadır.

# master_id: Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

###############################################################
# GÖREVLER
###############################################################

# GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama
           # 1. flo_data_20K.csv verisini okuyunuz.
import pandas as pd
df_=pd.read_csv("DERSLER/CRM/Case Study-FLOMusteriSegmentasyonu/FLOMusteriSegmentasyonu/flo_data_20k.csv")
df=df_.copy()
pd.set_option("display.max_columns", None)
df.head()

# 2. Veri setinde
                     # a. İlk 10 gözlem
df.head(10)
                     # b. Değişken isimleri,
df.columns
                     # c. Betimsel istatistik,
df.describe().T
                     # d. Boş değer,
df.isnull().sum() #yok
                     # e. Değişken tipleri, incelemesi yapınız.
df.dtypes
df.head()
#master_id cardinal, date icerenler date data türü fakat dfde hepsi object.
df.head()
           # 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Herbir müşterinin toplam
           # alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.
           df["total_value"] = df["customer_value_total_ever_offline"]+ df["customer_value_total_ever_online"]
           df["total_order"] = df["order_num_total_ever_offline"] + df["order_num_total_ever_online"]
           df.head()
           # 4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
           df["first_order_date"] = pd.to_datetime(df["first_order_date"])
           df["last_order_date"] = pd.to_datetime(df["last_order_date"])
           df["last_order_date_offline"] = pd.to_datetime(df["last_order_date_offline"])
           df["last_order_date_online"] = pd.to_datetime(df["last_order_date_online"])
           df.dtypes

           # 5.(?) Alışveriş kanallarındaki müşteri sayısının, ortalama alınan ürün sayısının ve ortalama harcamaların dağılımına bakınız.
           df.groupby("order_channel").agg({"master_id": "count",
                                        "total_order": "mean",
                                        "total_value":"mean"})
           df.head()
           # 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
           df.sort_values("total_value",ascending=False).head(10)
           # 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
           df.sort_values("total_order", ascending=False).head(10)
           # 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.
def rfm_prep(dff):
           dff["total_value"] = dff["customer_value_total_ever_offline"]+ dff["customer_value_total_ever_online"]
           dff["total_order"] = dff["order_num_total_ever_offline"] + dff["order_num_total_ever_online"]
           dff["first_order_date"] = pd.to_datetime(dff["first_order_date"])
           dff["last_order_date"] = pd.to_datetime(dff["last_order_date"])
           dff["last_order_date_offline"] = pd.to_datetime(dff["last_order_date_offline"])
           dff["last_order_date_online"] = pd.to_datetime(dff["last_order_date_online"])
           return dff
df = df_.copy()
rfm_prep(df)



# GÖREV 2: RFM Metriklerinin Hesaplanması
#recency: (son alisverisinin oranı) freq: alısveris sıklıgı  monetary:toplam odenen
import datetime as dt
df["last_order_date"].max()  #last date: ('2021-05-30 00:00:00')
today_date = dt.datetime(2021, 6, 2)
rfm = pd.DataFrame()
rfm["recency"] = today_date - df["last_order_date"]
rfm["frequency"] = df["total_order"]
rfm["monetary"] = df["total_value"]
rfm["customer id"] = df["master_id"]
df.head()
rfm.head()
#rfm['recency'] = rfm['recency'].values.astype(float)
# GÖREV 3: RF ve RFM Skorlarının Hesaplanması
rfm["frequency_score"]= pd.qcut(rfm["frequency"].rank(method="first"),5,labels=["1","2","3","4","5"])
rfm["monetary_score"]= pd.qcut(rfm["monetary"],5,labels=["1","2","3","4","5"])
rfm["recency_score"]= pd.qcut(rfm["recency"],5,labels=["5","4","3","2","1"])
rfm["rf_score"] = (rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str))
rfm.head()
# GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması
seg_map = {

    r"[1-2][1-2]": "hibernating",

    r"[1-2][3-4]": "at_risk",

    r"[1-2]5": "cant_loose",

    r"3[1-2]": "about_to_sleep",

    r"33": "need_attention",

    r"[3-4][4-5]": "loyal_customers",

    r"41": "promising",

    r"51": "new_customers",

    r"[4-5][2-3]": "potential_loyalist",

    r"5[4-5]": "champions",

}
rfm["segment"] = rfm["rf_score"].replace(seg_map, regex=True)
rfm.head()
# GÖREV 5: Aksiyon zamanı!
           # 1. Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.
           rfm.groupby("segment").agg({"mean","count"})
           # 2. RFM analizi yardımı ile 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv ye kaydediniz.
                   # a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde. Bu nedenle markanın
                   # tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçeilmek isteniliyor. Sadık müşterilerinden(champions,loyal_customers),
                   # ortalama 250 TL üzeri ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kuralacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına
                   # yeni_marka_hedef_müşteri_id.cvs olarak kaydediniz.
champ_loyals = list(rfm[(((rfm['segment'] == "champions")
                               | (rfm['segment'] == "loyal_customers"))
                              & (rfm['monetary'] > 250))].index)
df.head()
target_cust= list(df[(df["interested_in_categories_12"] == "KADIN")] & (df["master_id"] in champ_loyals ))].index)
# b. Erkek ve Çoçuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşteri olan ama uzun süredir
                   # alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni gelen müşteriler özel olarak hedef alınmak isteniliyor. Uygun profildeki müşterilerin id'lerini csv dosyasına indirim_hedef_müşteri_ids.csv
                   # olarak kaydediniz.


# GÖREV 6: Tüm süreci fonksiyonlaştırınız.
def rfm_prep(dff):
    dff["total_value"] = dff["customer_value_total_ever_offline"] + dff["customer_value_total_ever_online"]
    dff["total_order"] = dff["order_num_total_ever_offline"] + dff["order_num_total_ever_online"]
    dff["first_order_date"] = pd.to_datetime(dff["first_order_date"])
    dff["last_order_date"] = pd.to_datetime(dff["last_order_date"])
    dff["last_order_date_offline"] = pd.to_datetime(dff["last_order_date_offline"])
    dff["last_order_date_online"] = pd.to_datetime(dff["last_order_date_online"])

    import datetime as dt
    df["last_order_date"].max()  # last date: ('2021-05-30 00:00:00')
    today_date = dt.datetime(2021, 6, 2)
    rfm = pd.DataFrame()
    rfm["recency"] = today_date - df["last_order_date"]
    rfm["frequency"] = df["total_order"]
    rfm["monetary"] = df["total_value"]
    rfm["customer id"] = df["master_id"]
    rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=["1", "2", "3", "4", "5"])
    rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels=["1", "2", "3", "4", "5"])
    rfm["recency_score"] = pd.qcut(rfm["recency"], 5, labels=["5", "4", "3", "2", "1"])
    rfm["rf_score"] = (rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str))
    seg_map = {

        r"[1-2][1-2]": "hibernating",

        r"[1-2][3-4]": "at_risk",

        r"[1-2]5": "cant_loose",

        r"3[1-2]": "about_to_sleep",

        r"33": "need_attention",

        r"[3-4][4-5]": "loyal_customers",

        r"41": "promising",

        r"51": "new_customers",

        r"[4-5][2-3]": "potential_loyalist",

        r"5[4-5]": "champions",

    }
    rfm["segment"] = rfm["rf_score"].replace(seg_map, regex=True)

    return dff



