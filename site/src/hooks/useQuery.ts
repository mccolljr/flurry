import { useEffect, useState, useCallback, useContext } from "preact/hooks";

import * as rx from "rxjs";
import * as ops from "rxjs/operators";
import * as rxFetch from "rxjs/fetch";
import { createContext } from "preact";

export interface Observation<T> {
  latest: T | null;
  loading: boolean;
}

const useObservable = <T extends any>(
  obs: rx.Observable<T> | null,
  initial: T | null = null
): Observation<T> => {
  const [latest, setLatest] = useState<T | null>(initial);
  const [loading, setLoading] = useState<boolean>(false);
  useEffect(() => {
    setLatest(null);
    if (obs == null) return;

    setLoading(true);
    const sub = obs.subscribe({
      next(val: T) {
        setLatest(val);
      },
      error(err: any) {
        setLatest(null);
        console.error(err);
      },
      complete() {
        setLoading(false);
      },
    });
    return () => sub.unsubscribe();
  }, [obs]);
  return { loading, latest };
};

export interface GQLResult<T> {
  data: NonNullable<T> | null;
  error: boolean;
}

export class GQLClient {
  constructor(protected readonly apiURL: string) {}

  fetch<
    T extends Record<string, any> = Record<string, any>,
    V extends Record<string, any> = Record<string, any>
  >(
    query: string,
    variables: V,
    retries: number = 0
  ): rx.Observable<GQLResult<T>> {
    return rxFetch
      .fromFetch(this.apiURL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, variables }),
      })
      .pipe(
        ops.delay(Math.random() * 4000),
        ops.retry(retries),
        ops.switchMap((resp) => {
          if (resp.status < 300) {
            return rx.from(
              resp.json().then((d) => ({ data: d, error: false }))
            );
          } else {
            return rx.from(
              resp.text().then((err) => {
                console.error(err);
                return { data: null, error: true };
              })
            );
          }
        }),
        ops.catchError((err) => {
          console.error(err);
          return rx.of({ error: true, data: null });
        })
      );
  }
}

export const GQLContext = createContext<GQLClient | null>(null);

export const useClient = (): GQLClient => {
  const client = useContext(GQLContext);
  if (client == null) {
    throw new Error("graphql client is not available");
  }
  return client;
};

export interface QueryResult<T extends any> {
  latest: GQLResult<T> | null;
  loading: boolean;
  refetch: () => Promise<void>;
}

export const useQuery = <
  T extends Record<string, any> = Record<string, any>,
  V extends Record<string, any> = Record<string, any>
>(
  query: string,
  variables: V,
  autoload: boolean = false
): QueryResult<T> => {
  const var_string = JSON.stringify(variables);
  const [obs, setObs] = useState<rx.Observable<GQLResult<T>> | null>(null);
  const { latest, loading } = useObservable(obs, { data: null, error: false });
  const client = useClient();
  const refetch = useCallback(async () => {
    setObs(() => client.fetch<T, V>(query, variables));
  }, [query, var_string]);
  useEffect(() => {
    if (autoload) refetch();
  }, [refetch, autoload]);
  return { latest, loading, refetch };
};
