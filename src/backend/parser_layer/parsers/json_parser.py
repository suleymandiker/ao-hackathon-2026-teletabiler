import json
from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer

class JsonParser:
    def __init__(self):
        self.ts = TimestampNormalizer()
        
        # YENİ EKLENEN: Node.js (Pino/Bunyan) gibi kütüphanelerden gelen 
        # sayısal log seviyelerini (30, 50 vb.) metne (INFO, ERROR) çevirmek için sözlük.
        self.numeric_levels = {
            "10": "TRACE",
            "20": "DEBUG",
            "30": "INFO",
            "40": "WARNING",
            "50": "ERROR",
            "60": "FATAL"
        }

    def parse(self, log):
        # ÖNLEM 2 ve 4: Gizli karakterleri ve alt satırlara bölünmüş JSON'u tek satıra indirge
        log = log.strip()
        
        try:
            data = json.loads(log)
            
            # .get() yerine .pop() kullanıyoruz. 
            # Böylece bu standart verileri data sözlüğünden (dictionary) alıp çıkartıyoruz.
            # Geriye sadece bilinmeyen/dinamik ekstra alanlar kalacak.
            
            # 1. Zaman Damgası
            raw_time = None
            for key in ("timestamp", "time", "@timestamp"):
                raw_time = data.pop(key, None)
                # Preserve existing key priority and fallback, including epoch zero.
                if raw_time or (type(raw_time) in (int, float) and raw_time == 0):
                    break
            
            # 2. Seviye
            severity = data.pop("level", None) or data.pop("severity", None) or data.pop("status", "INFO")
            
            # YENİ EKLENEN: Sayısal seviyeyi temizle ve sözlükten (mapping) karşılığını bul
            severity_str = str(severity).strip().upper()
            final_severity = self.numeric_levels.get(severity_str, severity_str)
            
            # 3. Mesaj
            message = data.pop("message", None) or data.pop("msg", None)
            
            # Eğer logda özel bir mesaj alanı yoksa, geriye kalan tüm veriyi mesaj olarak kabul et
            if not message:
                message = str(data)

            return {
                # ÖNLEM 3: Normalizer ile zaman damgası UTC'ye standartlaştırılıyor
                "timestamp": self.ts.normalize(str(raw_time)) if raw_time is not None else None,
                "raw_timestamp": raw_time,
                
                # ÖNLEM 2 (Ekstra Temizlik): Çevrilmiş ve temizlenmiş seviyeyi ata
                "severity": final_severity,
                
                "message": str(message).strip(),
                
                # ÖNLEM 5: Mapping Explosion Koruması
                # data sözlüğünün içinde sadece user_id, duration_ms gibi dinamik alanlar kaldı.
                # Hepsini güvenli bir şekilde "attributes" altına gömüyoruz.
                "attributes": data
            }
        
        # Orijinal koddaki çıplak "except:" yerine, sadece JSON hatalarını yakalayan
        # daha güvenli ve Pythonic olan ValueError'u kullanıyoruz.
        except ValueError:
            return None
