# Chatterino bridge

Servicio Node para leer eventos de TikTok y YouTube desde clientes que consultan `/poll`.

## Endpoints

- `GET /health` confirma que el servicio responde.
- `GET /poll?platform=tiktok&channel=usuario&since=0` abre o consulta una conexión.
- `GET /poll?platform=youtube&channel=URL_O_ID&since=0` consulta YouTube.

Las solicitudes requieren `Authorization: Bearer <CHAT_BRIDGE_TOKEN>` cuando la variable `CHAT_BRIDGE_TOKEN` está configurada.

## Despliegue en Render

1. Crea un servicio con **New + → Blueprint** y selecciona este repositorio.
2. Render lee `render.yaml`, instala las dependencias y genera `CHAT_BRIDGE_TOKEN`.
3. Espera a que el estado sea **Live** y visita `https://TU-SERVICIO.onrender.com/health`.
4. Conserva la URL y el token en privado. El cliente debe usar ambos para hacer consultas al puente.

El puente reintenta desconexiones de TikTok y vence una conexión inicial que permanezca esperando más de 30 segundos.