FROM elixir:1.18-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git build-base

COPY mix.exs mix.lock ./
RUN mix local.hex --force && \
    mix local.rebar --force && \
    mix deps.get --only prod

COPY lib ./lib
RUN mix deps.compile && \
    MIX_ENV=prod mix compile

FROM elixir:1.18-alpine

WORKDIR /app

RUN apk add --no-cache libstdc++ openssl ncurses-libs

COPY --from=builder /app/_build /app/_build
COPY --from=builder /app/deps /app/deps
COPY --from=builder /app/lib /app/lib
COPY --from=builder /app/mix.exs /app/mix.lock ./

RUN mix local.hex --force && \
    mix local.rebar --force

ENV MIX_ENV=prod
ENV PORT=4000

RUN mkdir -p /app/data
VOLUME /app/data

EXPOSE 4000

CMD ["mix", "run", "--no-halt"]
