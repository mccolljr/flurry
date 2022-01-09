import { FunctionComponent, h } from "preact";
import { useState } from "preact/hooks";
import { useQuery } from "../../hooks/useQuery";
import {
  RegisterUserMutation as CommandResult,
  RegisterUserMutationVariables as CommandVars,
} from "../../gql-types";

import Container from "@material-ui/core/Container";
import TextField from "@material-ui/core/TextField";
import Button from "@material-ui/core/Button";

const registerQuery = `
mutation (
  $email: String!
  $lastName: String!
  $firstName: String!
  $username: String!
  $password: String!
) {
  CreateUserCommand(
    email: $email
    firstName: $firstName
    lastName: $lastName
    username: $username
    password: $password
  ) {
    ok
  }
}
`;

const Register: FunctionComponent = () => {
  const [form, setForm] = useState<CommandVars>({
    email: "",
    username: "",
    password: "",
    lastName: "",
    firstName: "",
  });
  const { latest, loading, refetch } = useQuery<CommandResult, CommandVars>(
    registerQuery,
    form
  );
  return (
    <Container is="div">
      <TextField
        label="Email"
        id="email"
        type="text"
        value={form.email}
        onChange={(evt) =>
          setForm((f) => ({
            ...f,
            email: (evt.target as HTMLInputElement).value,
          }))
        }
      />
      <TextField
        label="Username"
        id="username"
        type="text"
        value={form.username}
        onChange={(evt) =>
          setForm((f) => ({
            ...f,
            username: (evt.target as HTMLInputElement).value,
          }))
        }
      />
      <TextField
        label="Password"
        id="password"
        type="password"
        value={form.password}
        onChange={(evt) =>
          setForm((f) => ({
            ...f,
            password: (evt.target as HTMLInputElement).value,
          }))
        }
      />
      <Button onClick={() => refetch()} disabled={loading}>
        Register
      </Button>
      <pre>{latest && latest.error && "Something went wrong"}</pre>
    </Container>
  );
};

export default Register;
