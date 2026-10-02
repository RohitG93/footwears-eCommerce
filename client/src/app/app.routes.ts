import { Routes } from '@angular/router';
import { ShopComponent } from './features/shop/shop.component';
import { HomeComponent } from './features/home/home.component';
import { ProductDetailsComponent } from './features/shop/product-details/product-details.component';
import { CartComponent } from './features/cart/cart.component';
import { CheckoutComponent } from './features/checkout/checkout.component';
import { LoginComponent } from './features/account/login/login.component';
import { RegisterComponent } from './features/account/register/register.component';
import { authguardGuard } from './core/guards/authguard-guard';
import { emptyGuard } from './core/guards/empty-guard';
import { CheckoutSuccessComponent } from './features/checkout/checkout-success/checkout-success.component';
import { OrderComponent } from './features/order/order.component';
import { OrderDetailComponent } from './features/order/order-detail/order-detail.component';
import { orderCompleteGuardGuard } from './core/guards/order-complete-guard-guard';

export const routes: Routes = [
    {path: '', component: HomeComponent},
    {path: 'shop', component: ShopComponent},
    {path: 'cart', component: CartComponent},
    {path: 'checkout', component: CheckoutComponent, canActivate: [authguardGuard, emptyGuard] },
    {path: 'checkout/success', component: CheckoutSuccessComponent, canActivate: [authguardGuard, orderCompleteGuardGuard] },
    {path: 'orders', component: OrderComponent, canActivate: [authguardGuard] },
    {path: 'order/:id', component: OrderDetailComponent, canActivate: [authguardGuard] },
    {path:'shop/:id', component: ProductDetailsComponent},
    {path: 'account/login', component: LoginComponent},
    {path: 'account/register', component: RegisterComponent},
    {path: '**', redirectTo: '', pathMatch: 'full'},
];
