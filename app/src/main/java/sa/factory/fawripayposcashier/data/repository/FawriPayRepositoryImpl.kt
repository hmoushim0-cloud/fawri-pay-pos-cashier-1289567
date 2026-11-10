package sa.factory.fawripayposcashier.data.repository

import sa.factory.fawripayposcashier.domain.repository.FawriPayRepository
import sa.factory.fawripayposcashier.hardware.HardwareSensorDriver
import sa.factory.fawripayposcashier.model.FawriPayItem
import sa.factory.fawripayposcashier.model.FawriPayOrder
import sa.factory.fawripayposcashier.model.FawriPayTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class FawriPayRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : FawriPayRepository {

    private val _catalog = MutableStateFlow<List<FawriPayItem>>(listOf(
        FawriPayItem("1", "إعداد متجر سالح الأساسي", "ضبط إعدادات المتجر الأولية وربط بوابات الدفع", 550.0, "معتمد", "إدارة المخزون"),
        FawriPayItem("2", "تكامل الطابعة الحرارية", "تهيئة طابعة ESC/POS مع قوالب فواتير مخصصة", 380.0, "فوري", "تقارير المبيعات"),
        FawriPayItem("3", "تدريب الموظفين على النظام", "جلسة تفاعلية لإدارة المخزون ومسح الباركود", 220.0, "جديد", "إعدادات الطابعة")
    ))
    override val catalogFlow: Flow<List<FawriPayItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<FawriPayItem>>(emptyList())
    override val activeCart: StateFlow<List<FawriPayItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<FawriPayTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            FawriPayTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: FawriPayItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: FawriPayItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): FawriPayOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = FawriPayOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
