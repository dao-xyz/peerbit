import { client as api, getPort } from "@dao-xyz/peerbit-node";
import { useEffect, useState } from "react";
import { Box, Grid, Paper, Typography } from "@mui/material";
import {
	createTheme,
	responsiveFontSizes,
	ThemeProvider,
	CssBaseline,
} from "@mui/material";
let theme = createTheme({
	palette: {
		mode: "dark",
	},
	typography: {
		fontFamily: [
			"-apple-system",
			"BlinkMacSystemFont",
			'"Segoe UI"',
			"Roboto",
			'"Helvetica Neue"',
			"Arial",
			"sans-serif",
			'"Apple Color Emoji"',
			'"Segoe UI Emoji"',
			'"Segoe UI Symbol"',
		].join(","),
	},
});
theme = responsiveFontSizes(theme);

export const App = () => {
	const [client, setClient] = useState<
		Awaited<ReturnType<typeof api>> | undefined
	>();
	const [password, setPassword] = useState<string>();
	const [id, setId] = useState<string>();
	useEffect(() => {
		console.log();

		api(
			window.location.protocol +
				"//" +
				window.location.hostname +
				":" +
				getPort(window.location.protocol)
		).then((c) => {
			setClient(c);
			c.peer.id
				.get()
				.then((_id) => {
					setId(_id);
				})
				.catch((e) => {
					if (window.location.hostname !== "localhost") {
						alert(e);
					} else {
						console.error(e);
					}
				});
		});
	}, []);
	return (
		<ThemeProvider theme={theme}>
			<CssBaseline />
			<Box>
				<Grid container sx={{ p: 4, height: "100vh" }}>
					<Grid item container direction="column" spacing={2} maxWidth="400px">
						<Grid item container direction="row" alignItems="center" mb={2}>
							<Grid mr={2} display="flex" justifyContent="center" item>
								<img width="30px" height="auto" src="./logo192.png"></img>
							</Grid>
							<Grid item>
								<Typography variant="h5">Peerbit</Typography>
							</Grid>
						</Grid>
						<Grid item>
							<Typography variant="overline">Id</Typography>
							<Typography>{id}</Typography>
						</Grid>
						<Grid item>
							<Typography variant="overline">Address</Typography>
						</Grid>
						<Grid item>
							<Typography variant="caption"> TCP (non-browser)</Typography>

							<Paper elevation={10}>
								<Typography
									m={2}
									sx={{ verticalAlign: "middle" }}
									variant="caption"
								>
									{" "}
									/dns4/
									{window.location.hostname === "localhost"
										? "127.0.0.1"
										: window.location.hostname}
									/tcp/4002/p2p/{id}
								</Typography>
							</Paper>
						</Grid>
						<Grid item>
							<Typography variant="caption">
								{" "}
								Websockets (browser and non-browser){" "}
							</Typography>
							<Paper elevation={10}>
								<Typography
									m={2}
									sx={{ verticalAlign: "middle" }}
									variant="caption"
								>
									/dns4/
									{window.location.hostname === "localhost"
										? "127.0.0.1"
										: window.location.hostname}
									/tcp/4003/wss/p2p/{id}
								</Typography>
							</Paper>
						</Grid>
					</Grid>
				</Grid>
			</Box>
		</ThemeProvider>
	);
};
