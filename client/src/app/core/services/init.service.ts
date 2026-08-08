import { inject, Service } from '@angular/core';
import { CartService } from './cart.service';
import { of } from 'rxjs';

@Service()
export class InitService {
    private cartService = inject(CartService)

    init() {
        const cart_id = localStorage.getItem('cart_id');
        const carts = cart_id ?  this.cartService.getCart(cart_id) : of(null);

        return carts;
    }
}
