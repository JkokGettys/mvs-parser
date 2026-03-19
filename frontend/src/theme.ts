import { createTheme } from '@mui/material/styles'

const MAIN_COLOR = '#4260D3'

const theme = createTheme({
  palette: {
    primary: {
      main: MAIN_COLOR,
      light: '#6B83DF',
      dark: '#2E4394',
      contrastText: '#fff',
    },
    secondary: {
      main: '#686B6B',
      light: '#9e9e9e',
      dark: '#424242',
    },
    error: {
      main: '#E4002B',
    },
    success: {
      main: '#008b7d',
    },
    warning: {
      main: '#ed6c02',
    },
    text: {
      primary: '#2E4154',
      secondary: '#60768b',
    },
    background: {
      default: '#f5f6fa',
      paper: '#ffffff',
    },
  },
  typography: {
    fontFamily: '"Nunito Sans", sans-serif',
    h4: {
      fontWeight: 700,
    },
    h5: {
      fontWeight: 700,
    },
    h6: {
      fontWeight: 700,
    },
    subtitle1: {
      fontWeight: 600,
    },
    button: {
      fontWeight: 600,
      textTransform: 'none',
    },
  },
  shape: {
    borderRadius: 8,
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: 6,
          padding: '8px 20px',
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          borderRadius: 8,
        },
      },
      defaultProps: {
        elevation: 0,
        variant: 'outlined',
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          fontWeight: 600,
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        head: {
          fontWeight: 700,
          color: '#2E4154',
        },
      },
    },
  },
})

export default theme
