import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import VersionListPage from './pages/VersionListPage'
import VersionDetailPage from './pages/VersionDetailPage'

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<VersionListPage />} />
        <Route path="/versions/:id" element={<VersionDetailPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Layout>
  )
}
