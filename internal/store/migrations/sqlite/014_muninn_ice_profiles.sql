-- Preserve the former built-in relay only for its associated public Muninn.
-- Other Muninn origins have no implicit third-party STUN/TURN configuration.
INSERT INTO config_values (name, value)
SELECT 'muninn_ice', '{"https://muninn.evil-bread.ru":{"stun_urls":["stun:stun.evil-bread.ru:3478"],"turn_urls":["turn:turn.evil-bread.ru:3478?transport=udp","turn:turn.evil-bread.ru:3478?transport=tcp","turns:turn.evil-bread.ru:5349?transport=tcp"],"turn_username":"turnuser","turn_password":"turnpass"}}'
WHERE EXISTS (
    SELECT 1 FROM config_values AS settings
    WHERE (settings.name = 'muninn' AND rtrim(settings.value, '/') = 'https://muninn.evil-bread.ru')
       OR (settings.name = 'muninn_servers' AND EXISTS (
           SELECT 1 FROM json_each(CASE WHEN json_valid(settings.value) THEN settings.value ELSE '[]' END)
           WHERE rtrim(json_each.value, '/') = 'https://muninn.evil-bread.ru'
       ))
)
ON CONFLICT(name) DO NOTHING;
