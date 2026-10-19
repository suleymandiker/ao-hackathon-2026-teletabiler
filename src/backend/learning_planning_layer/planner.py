from __future__ import annotations
from typing import Any, Dict, Iterable, List


class LearningPlanner:
    """Arıza türü ve kök nedene göre sorumlu ekibi, eylem planını ve kontrol adımlarını belirleyen operasyonel planlayıcı."""

    @staticmethod
    def _resolve_owner_and_action(inc: Dict[str, Any]) -> tuple[str, str, List[str]]:
        """Incident ve kök sinyal verilerinden sorumlu ekibi, operasyonel aksiyonu ve kontrol listesini çıkarır."""
        root = inc.get('probable_root') or {}
        kind = root.get('kind', '')
        alarm_type = str(root.get('alarm_type') or inc.get('alarm_type') or '').lower()
        entity = str(root.get('entity') or (inc.get('services') or ['unknown'])[0])
        tpl = str(inc.get('root_cause_template') or '').lower()

        # 1. Veritabanı Arızaları (OperationalError, connection pool, deadlock, db session)
        if (
            'database' in alarm_type
            or 'db_' in alarm_type
            or entity.endswith('-db')
            or any(k in tpl for k in ('operationalerror', 'psycopg', 'deadlock', 'closed the connection', 'can\'t reconnect'))
        ):
            return (
                'Database Operations',
                'verify_db_connection_pool_and_sessions',
                [
                    'Veritabanı sunucusunun aktif bağlantı sayısını ve bağlantı limitlerini kontrol et.',
                    'Kilitlenme (deadlock) veya anormal sonlanan oturumları (abnormal termination) incele.',
                    'İstemci tarafındaki connection pool boyutunu ve timeout ayarlarını gözden geçir.'
                ]
            )

        # 2. Ağ, DNS ve Altyapı İletişim Arızaları
        if (
            'network' in alarm_type
            or kind == 'infrastructure'
            or entity.endswith('-gw')
            or 'provider' in entity
            or any(k in tpl for k in ('connection refused', 'no route to host', 'broken pipe', 'socket closed'))
        ):
            return (
                'Network Operations',
                'check_network_connectivity_and_firewall',
                [
                    'Hedef servis/IP erişilebilirliğini ve DNS çözümlemesini test et.',
                    'Kubernetes NetworkPolicy ve firewall egress kurallarını kontrol et.',
                    'Kabin/switch bağlantılarını ve TCP socket durumlarını incele.'
                ]
            )

        # 3. Bellek, CPU ve Kaynak Tükenmesi (OOM)
        if (
            'oom' in alarm_type
            or 'resource' in alarm_type
            or any(k in tpl for k in ('out of memory', 'oomkilled', 'heap space', 'gc_pressure'))
        ):
            return (
                'Platform / Infrastructure SRE',
                'inspect_pod_memory_and_restart_history',
                [
                    'Pod bellek limitlerini (requests/limits) gözden geçir.',
                    'Exit Code 137 (OOMKilled) ve kernel dmesg kayıtlarını incele.',
                    'Bellek sızıntısı (memory leak) şüphesi için heap dump analizini başlat.'
                ]
            )

        # 4. Bulut Depolama ve S3 / Blob Arızaları
        if (
            'storage' in alarm_type
            or 'ext_' in alarm_type
            or any(k in tpl for k in ('nosuchkey', 'bucket', 'failed to download', 's3:'))
        ):
            return (
                'Integration / Storage Operations',
                'verify_storage_credentials_and_bucket_access',
                [
                    'S3/Blob IAM rollerini ve erişim anahtarlarını doğrula.',
                    'İlgili dosya yolunun (bucket/key) depolama alanında varlığını teyit et.',
                    'Depolama bant genişliği ve kota sınırlarını kontrol et.'
                ]
            )

        # 5. Uygulama İçi Kod Hataları / Unhandled Exception
        if any(k in tpl for k in ('traceback', 'panic:', 'nullpointer', 'keyerror', 'indexerror')):
            svc_name = entity if entity != 'unknown' else 'Application'
            return (
                f'{svc_name} Service Owner',
                'investigate_application_traceback_and_rollback',
                [
                    'Son dağıtıma (deployment) ait commit ve PR değişikliklerini incele.',
                    'Hata veren fonksiyonun girdi parametrelerini doğrula.',
                    'Gerekirse bir önceki stabil release sürümüne geri alma (rollback) planla.'
                ]
            )

        # 6. Genel Fallback
        owner = f'{entity} Service Owner' if entity != 'unknown' else 'SRE / NOC'
        return (
            owner,
            'prepare_operator_review',
            ['Olay loglarını ve telemetri metriklerini operatör incelemesi için hazırla.']
        )

    def plan(self, incidents: Iterable[Dict[str, Any]], rca: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rmap = {r['incident_id']: r for r in rca if isinstance(r, dict) and 'incident_id' in r}
        out = []

        for inc in incidents:
            inc_id = inc.get('incident_id')
            confidence = float(inc.get('confidence', 0.85))

            # Sorumlu ekip, aksiyon kodu ve kontrol listesini belirle
            owner, action, checklist = self._resolve_owner_and_action(inc)

            # Doğru açılış zamanı (start_ms öncelikli)
            opened_at = inc.get('start_ms') or inc.get('first_seen_ms') or inc.get('end_ms')

            # Vaka durumunu ActiveIncidentStore'dan gelen dinamik değerle senkronize et
            status = inc.get('status', 'Açık')

            out.append({
                'incident_id': inc_id,
                'recommended_next_step': action,
                'owner': owner,
                'status': status,
                'opened_at_ms': opened_at,
                'closed_at_ms': None,
                'execution_allowed': False,
                'requires_human_approval': True,
                'action_checklist': checklist,
                'evidence_count': int(inc.get('signal_count', 0)),
                'confidence': confidence,
                'rca': rmap.get(inc_id, {})
            })

        return out