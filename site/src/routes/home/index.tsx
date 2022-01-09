import { FunctionalComponent, h } from "preact";
import { useQuery } from "../../hooks/useQuery";
import AuthGuard from "../../components/auth-guard";

import {
  LoadUserQuery as QueryResult,
  LoadUserQueryVariables as QueryVars,
} from "../../gql-types";

import style from "./style.scss";

const userQuery = `
query LoadUser($username: String!) {
  FindUserByUsernameQuery(username: $username) {
    user {
      email
      username
      lastLogin
    }
  }
}
`;

const Home: FunctionalComponent = () => {
  const { latest, loading } = useQuery<QueryResult, QueryVars>(
    userQuery,
    { username: "mccolljr" },
    true
  );
  return (
    <div class={style.home}>
      <h1>Home</h1>
      <AuthGuard
        guard={(auth) => auth.authenticated}
        fallback="not authenticated"
      >
        authenticated
      </AuthGuard>
      {loading ? (
        <p>loading...</p>
      ) : (
        <p>
          {latest?.data?.FindUserByUsername?.user
            ? JSON.stringify(latest.data?.FindUserByUsername.user)
            : "something went wrong"}
        </p>
      )}
    </div>
  );
};

export default Home;
