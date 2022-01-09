import { createContext } from "preact";
import { useContext, useMemo } from "preact/hooks";
import { LoadUserQuery } from "../gql-types";

export interface AuthContextValue {
  authenticated: boolean;
  userData: LoadUserQuery | null;
}

export const AuthContext = createContext<AuthContextValue>({
  authenticated: false,
  userData: null,
});

export const useAuth = (): AuthContextValue => {
  return useContext(AuthContext);
};

export interface AuthPredicate {
  (ctx: AuthContextValue): boolean;
}

export const useAuthGuard = (guard: AuthPredicate): boolean => {
  const authCtx = useAuth();
  return guard(authCtx);
};
