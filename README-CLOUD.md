# Puente TikTok para la APK

Este servicio ejecuta el mismo método que el `.exe`: `tiktok-live-connector`.
La APK consulta `/poll` y el servidor mantiene la conexión con TikTok.

## Despliegue en Render

1. Sube esta carpeta a un repositorio privado de GitHub.
2. En Render elige **New Blueprint** y selecciona el repositorio.
3. Render usará `render.yaml` y generará `CHAT_BRIDGE_TOKEN` automáticamente.
4. Copia la URL `https://...onrender.com` y el valor de `CHAT_BRIDGE_TOKEN`.

La URL de prueba es `/health`. El servicio puede tardar unos segundos en despertar
en el plan gratuito.

Para construir la APK:

```powershell
flutter build apk --release `
  --dart-define=TIKTOK_BRIDGE_URL=https://TU-SERVICIO.onrender.com `
  --dart-define=TIKTOK_BRIDGE_TOKEN=TU_TOKEN
```
