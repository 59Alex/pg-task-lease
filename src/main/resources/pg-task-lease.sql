CREATE TABLE public.pg_task_lease (
    id uuid PRIMARY KEY,
    queue varchar(120) NOT NULL CHECK (length(queue) > 0),
    payload text NOT NULL,
    priority integer NOT NULL DEFAULT 0,
    state varchar(8) NOT NULL DEFAULT 'READY' CHECK (state IN ('READY', 'LEASED', 'DONE', 'DEAD')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts > 0),
    generation bigint NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_token uuid,
    owner varchar(200),
    lease_until timestamptz,
    last_error varchar(2048),
    CHECK (attempts <= max_attempts),
    CHECK ((state = 'LEASED' AND lease_token IS NOT NULL AND owner IS NOT NULL AND lease_until IS NOT NULL)
        OR (state <> 'LEASED' AND lease_token IS NULL AND owner IS NULL AND lease_until IS NULL))
);

CREATE INDEX pg_task_lease_ready ON public.pg_task_lease(queue, priority DESC, created_at, id)
    WHERE state = 'READY';
CREATE INDEX pg_task_lease_expired ON public.pg_task_lease(queue, lease_until)
    WHERE state = 'LEASED';
