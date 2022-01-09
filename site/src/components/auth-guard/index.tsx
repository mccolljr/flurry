import { ComponentChildren, FunctionalComponent, Fragment, h } from "preact";
import { AuthPredicate, useAuthGuard } from "../../hooks/useAuth";

export interface AuthGuardProps {
  guard: AuthPredicate;
  children: ComponentChildren;
  fallback: ComponentChildren;
}

const AuthGuard = ({
  guard,
  children = null,
  fallback = null,
}: AuthGuardProps) => {
  const guardSucceeded = useAuthGuard(guard);
  return <>{guardSucceeded ? children : fallback}</>;
};

export default AuthGuard;
