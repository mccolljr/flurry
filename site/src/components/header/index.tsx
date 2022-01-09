import { FunctionalComponent, h } from "preact";
import { route } from "preact-router";
import AppBar from "@material-ui/core/AppBar";
import Button from "@material-ui/core/Button";
import Toolbar from "@material-ui/core/Toolbar";
import Typography from "@material-ui/core/Typography";

import style from "./style.scss";

const Header: FunctionalComponent = () => {
  return (
    <AppBar position="static">
      <Toolbar>
        <Typography
          variant="h6"
          className={style.title}
          onClick={() => route("/")}
        >
          Money {process.env.API_URL}
        </Typography>
        <div className={style.spacer}></div>
        <Button
          variant="text"
          color="inherit"
          onClick={() => route("/register")}
        >
          Register
        </Button>
      </Toolbar>
    </AppBar>
  );
};

export default Header;
