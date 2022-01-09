import { FunctionalComponent, h } from "preact";
import { Route, Router } from "preact-router";
import ThemeProvider from "@material-ui/styles/ThemeProvider";
import { createTheme } from "@material-ui/core/styles";
import Home from "../routes/home";
import Register from "../routes/register";
import Header from "./header";
import { GQLClient, GQLContext } from "../hooks/useQuery";
import { AuthContext } from "../hooks/useAuth";

const DefaultTheme = createTheme({});
const DefaultClient = new GQLClient("http://localhost:8080/");

const App: FunctionalComponent = () => {
  return (
    <div id="preact_root">
      <ThemeProvider theme={DefaultTheme}>
        <GQLContext.Provider value={DefaultClient}>
          <AuthContext.Provider
            value={{ userData: null, authenticated: false }}
          >
            <Header />
            <Router>
              <Route path="/register" component={Register} />
              <Route default component={Home} />
            </Router>
          </AuthContext.Provider>
        </GQLContext.Provider>
      </ThemeProvider>
    </div>
  );
};

export default App;
