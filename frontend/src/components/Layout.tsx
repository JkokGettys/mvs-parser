import { ReactNode } from 'react'
import {
  AppBar,
  Toolbar,
  Typography,
  Container,
  Box,
  Chip,
} from '@mui/material'
import { useNavigate } from 'react-router-dom'

interface LayoutProps {
  children: ReactNode
}

export default function Layout({ children }: LayoutProps) {
  const navigate = useNavigate()

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', minHeight: '100vh', bgcolor: 'background.default' }}>
      <AppBar
        position="sticky"
        elevation={0}
        sx={{
          bgcolor: '#fff',
          borderBottom: '1px solid',
          borderColor: 'divider',
        }}
      >
        <Toolbar>
          <Box
            sx={{ display: 'flex', alignItems: 'center', gap: 1.5, cursor: 'pointer' }}
            onClick={() => navigate('/')}
          >
            <Box
              sx={{
                width: 32,
                height: 32,
                borderRadius: 1,
                bgcolor: 'primary.main',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: '#fff',
                fontWeight: 800,
                fontSize: 14,
              }}
            >
              M
            </Box>
            <Typography variant="h6" sx={{ color: 'text.primary', fontSize: 18 }}>
              MVS Parser Admin
            </Typography>
          </Box>
          <Box sx={{ flexGrow: 1 }} />
          <Chip
            label="Internal Tool"
            size="small"
            variant="outlined"
            sx={{ color: 'text.secondary', borderColor: 'divider' }}
          />
        </Toolbar>
      </AppBar>

      <Container maxWidth="lg" sx={{ py: 4, flex: 1 }}>
        {children}
      </Container>

      <Box
        component="footer"
        sx={{
          py: 2,
          textAlign: 'center',
          borderTop: '1px solid',
          borderColor: 'divider',
          bgcolor: '#fff',
        }}
      >
        <Typography variant="caption" color="text.secondary">
          Bowery Valuation - MVS Parser Service
        </Typography>
      </Box>
    </Box>
  )
}
